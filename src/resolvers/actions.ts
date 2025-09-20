import Context, { ConfigAccess } from '../context'
import { ModelsManager } from '../models/manager'
import { sequelize } from '../models'
import { Action } from '../models/actions'
import { Item } from '../models/items'
import { diff, isObjectEmpty, mergeValues, processItemButtonActions, processItemButtonActions2, processTableButtonActions, testAction } from './utils'
import audit, { AuditItem, ChangeType } from '../audit'

/** =========================
 *  IF/THEN Rule: Types + Compiler
 *  ========================= */
type ConditionOp =
  | 'eq' | 'neq' | 'contains' | 'ncontains' | 'empty' | 'nempty'
  | 'gt' | 'lt' | 'gte' | 'lte' | 'regex'

interface Condition {
  left: string
  op: ConditionOp
  right?: string | number | boolean | null
}

type ActionKind = 'set' | 'append' | 'incr' | 'remove'

interface ThenAction {
  target: string
  kind: ActionKind
  value?: string | number | boolean | null
}

interface RulePayload {
  type: number
  event: number
  itemType?: number
  itemFrom?: number
  relation?: number
  itemButton?: string
  askBeforeExec?: boolean
  roles?: number[]
  conditionsJoin: 'AND' | 'OR'
  conditions: Condition[]
  actions: ThenAction[]
}

/** ---- Helpers to compile rules into executable code ---- */

function validateRule(rule?: RulePayload) {
  if (!rule) return
  if (!rule.event) throw new Error('Rule must have an event')
  if (!rule.conditions?.length) throw new Error('Rule must have at least one condition')
  if (!rule.actions?.length) throw new Error('Rule must have at least one action')

  for (const c of rule.conditions) {
    if (!c.left || !c.op) throw new Error('Each condition requires "left" and "op"')
  }
  for (const a of rule.actions) {
    if (!a.target || !a.kind) throw new Error('Each action requires "target" and "kind"')
  }
}

function escBackticks(s: string) {
  return String(s).replace(/\\/g, '\\\\').replace(/`/g, '\\`')
}

function jsValue(v: any): string {
  if (v === true || v === false) return String(v)
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'number') return String(v)
  if (!isNaN(v as any) && v !== '') return String(v)
  return '`' + escBackticks(v) + '`'
}

function pathToJs(path: string): string {
  // "values.Color" => item.values["Color"]
  // "values.name[lang]" => item.values.name[lang]
  if (!path) return 'item'
  let out = 'item'
  const dot = path.split('.')
  for (const seg of dot) {
    if (!seg) continue
    if (seg.includes('[')) out += '.' + seg
    else if (/^[a-zA-Z_]\w*$/.test(seg)) out += '.' + seg
    else out += `["${seg.replace(/"/g, '\\"')}"]`
  }
  return out
}

function renderCondition(c: Condition): string {
  const left = pathToJs(c.left)
  switch (c.op) {
    case 'eq':  return `${left} === ${jsValue(c.right)}`
    case 'neq': return `${left} !== ${jsValue(c.right)}`
    case 'contains': return `${left} && String(${left}).includes(${jsValue(c.right)})`
    case 'ncontains': return `!(${left} && String(${left}).includes(${jsValue(c.right)}))`
    case 'empty': return `!${left} || ${left} === ''`
    case 'nempty': return `${left} && ${left} !== ''`
    case 'gt': return `Number(${left}) > Number(${jsValue(c.right)})`
    case 'lt': return `Number(${left}) < Number(${jsValue(c.right)})`
    case 'gte': return `Number(${left}) >= Number(${jsValue(c.right)})`
    case 'lte': return `Number(${left}) <= Number(${jsValue(c.right)})`
    case 'regex': return `new RegExp(${jsValue(c.right)}).test(String(${left}))`
    default: return 'true'
  }
}

function renderActions(arr: ThenAction[]): { code: string; touchValues: boolean; touchRuntime: boolean } {
  const lines: string[] = []
  let touchValues = false
  let touchRuntime = false

  for (const a of arr) {
    const tgt = pathToJs(a.target)
    if (a.target.startsWith('values')) touchValues = true
    if (a.target.startsWith('runtime')) touchRuntime = true

    switch (a.kind) {
      case 'set':
        lines.push(`${tgt} = ${jsValue(a.value)};`)
        break
      case 'append':
        lines.push(`${tgt} = Array.isArray(${tgt}) ? [...${tgt}, ${jsValue(a.value)}] : String(${tgt} || '') + ${jsValue(a.value)};`)
        break
      case 'incr':
        lines.push(`${tgt} = Number(${tgt} || 0) + Number(${jsValue(a.value)});`)
        break
      case 'remove':
        lines.push(`${tgt} = null;`)
        break
    }
  }

  if (touchValues) lines.push(`item.changed('values', true);`)
  if (touchRuntime) lines.push(`item.changed('runtime', true);`)

  return { code: lines.join('\n'), touchValues, touchRuntime }
}

function buildActionCodeFromRule(rule?: RulePayload): string {
  if (!rule) return ''
  const joiner = rule.conditionsJoin || 'AND'
  const condExpr = (rule.conditions || []).length
    ? rule.conditions.map(renderCondition).join(` ${joiner} `)
    : 'true'

  const acts = renderActions(rule.actions || [])
  const isAfter = [2, 4, 6, 10].includes(rule.event) // treat "after" events as saving points

  const header =
    rule.event === 0 ? '// Always' :
      rule.event === 1 ? '// Before Create' :
        rule.event === 2 ? '// After Create' :
          rule.event === 3 ? '// Before Update' :
            rule.event === 4 ? '// After Update' :
              rule.event === 11 ? '// Attribute Changed' :
                '// Event'

  return `${header}
if (${condExpr}) {
${acts.code.split('\n').map(l => '  ' + l).join('\n')}
${isAfter ? '  await item.save();\n' : ''}}`
}

function computeCodeFromInput(code?: string, rule?: RulePayload): string {
  if (code && code.trim()) return code
  if (rule) {
    validateRule(rule)
    return buildActionCodeFromRule(rule)
  }
  return ''
}

/** =========================
 *  Resolver
 *  ========================= */
export default {
  Query: {
    getActions: async (parent: any, args: any, context: Context) => {
      context.checkAuth()
      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)
      return mng.getActions()
    }
  },
  Mutation: {
    createAction: async (parent: any, { identifier, name, code, order, triggers, rule }: any, context: Context) => {
      context.checkAuth()
      if (!context.canEditConfig(ConfigAccess.ACTIONS))
        throw new Error('User ' + context.getCurrentUser()?.id + ' does not has permissions to create action, tenant: ' + context.getCurrentUser()!.tenantId)

      if (!/^[A-Za-z0-9_-]*$/.test(identifier))
        throw new Error('Identifier must not has spaces and must be in English only: ' + identifier + ', tenant: ' + context.getCurrentUser()!.tenantId)

      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)
      const tst = mng.getActions().find(act => act.identifier === identifier)
      if (tst) throw new Error('Identifier already exists: ' + identifier + ', tenant: ' + context.getCurrentUser()!.tenantId)

      const compiledCode = computeCodeFromInput(code, rule as RulePayload | undefined)

      const action = await sequelize.transaction(async (t) => {
        return await Action.create({
          identifier,
          tenantId: context.getCurrentUser()!.tenantId,
          createdBy: context.getCurrentUser()!.login,
          updatedBy: context.getCurrentUser()!.login,
          name,
          order: order != null ? order : 0,
          code: compiledCode || '',
          triggers: triggers || []
        }, { transaction: t })
      })

      mng.getActions().push(action)
      await mng.reloadModelRemotely(action.id, null, 'ACTION', false, context.getUserToken())
      return action.id
    },

    updateAction: async (parent: any, { id, name, code, order, triggers, rule }: any, context: Context) => {
      context.checkAuth()
      if (!context.canEditConfig(ConfigAccess.ACTIONS))
        throw new Error('User ' + context.getCurrentUser()?.id + ' does not has permissions to update action, tenant: ' + context.getCurrentUser()!.tenantId)

      const nId = parseInt(id)
      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)

      const act = mng.getActions().find(act => act.id === nId)
      if (!act) throw new Error('Failed to find action by id: ' + id + ', tenant: ' + mng.getTenantId())

      if (name != null) act.name = name
      if (order != null) act.order = order
      if (triggers) act.triggers = triggers

      // Prefer explicit code; else compile from rule when present
      if (code != null || rule) act.code = computeCodeFromInput(code, rule as RulePayload | undefined)

      act.updatedBy = context.getCurrentUser()!.login
      await sequelize.transaction(async (t) => {
        await act!.save({ transaction: t })
      })
      delete mng.getActionsCache()[act.identifier]
      await mng.reloadModelRemotely(act.id, null, 'ACTION', false, context.getUserToken())
      return act.id
    },

    removeAction: async (parent: any, { id }: any, context: Context) => {
      context.checkAuth()
      if (!context.canEditConfig(ConfigAccess.ACTIONS))
        throw new Error('User ' + context.getCurrentUser()?.id + ' does not has permissions to remove action, tenant: ' + context.getCurrentUser()!.tenantId)

      const nId = parseInt(id)
      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)

      const idx = mng.getActions().findIndex(act => act.id === nId)
      if (idx === -1) throw new Error('Failed to find action by id: ' + id + ', tenant: ' + mng.getTenantId())

      const act = mng.getActions()[idx]
      act.updatedBy = context.getCurrentUser()!.login

      // change identifier before deletion to allow reusing same identifier
      act.identifier = act.identifier + '_d_' + Date.now()
      await sequelize.transaction(async (t) => {
        await act!.save({ transaction: t })
        await act!.destroy({ transaction: t })
      })

      mng.getActions().splice(idx, 1)
      await mng.reloadModelRemotely(act.id, null, 'ACTION', true, context.getUserToken())
      return true
    },

    executeButtonAction: async (parent: any, { itemId, buttonText, data }: any, context: Context) => {
      context.checkAuth()

      const nId = parseInt(itemId)
      const item = await Item.applyScope(context).findByPk(nId)
      if (!item) throw new Error('Failed to find item by id: ' + nId + ', tenant: ' + context.getCurrentUser()!.tenantId)

      const { channels, values, result } = await processItemButtonActions(context, buttonText, item, data)

      if (!context.canEditItem(item)) return result

      let itemDiff: AuditItem
      if (audit.auditEnabled()) itemDiff = diff({ values: item.values }, { values: values })

      item.values = values
      item.changed("values", true)
      item.channels = channels

      item.updatedBy = context.getCurrentUser()!.login
      await sequelize.transaction(async (t) => {
        await item.save({ transaction: t })
      })

      if (audit.auditEnabled()) {
        if (!isObjectEmpty(itemDiff!.added) || !isObjectEmpty(itemDiff!.changed) || !isObjectEmpty(itemDiff!.deleted))
          audit.auditItem(ChangeType.UPDATE, item.id, item.identifier, itemDiff!, context.getCurrentUser()!.login, item.updatedAt)
      }

      return result
    },

    executeTableButtonAction: async (parent: any, { itemId, buttonText, where, headers, data }: any, context: Context) => {
      context.checkAuth()

      let item: (Item | null) = null
      if (itemId) {
        const nId = parseInt(itemId)
        item = await Item.applyScope(context).findByPk(nId)
        if (!item) throw new Error('Failed to find item by id: ' + nId + ', tenant: ' + context.getCurrentUser()!.tenantId)
      }

      const { channels, values, result } = await processTableButtonActions(context, buttonText, item, where, headers, data)

      if (item && !context.canEditItem(item)) return result

      if (item) {
        let itemDiff: AuditItem
        if (audit.auditEnabled()) itemDiff = diff({ values: item.values }, { values: values })

        item.values = values
        item.changed("values", true)
        item.channels = channels

        item.updatedBy = context.getCurrentUser()!.login
        await sequelize.transaction(async (t) => {
          await item!.save({ transaction: t })
        })

        if (audit.auditEnabled()) {
          if (!isObjectEmpty(itemDiff!.added) || !isObjectEmpty(itemDiff!.changed) || !isObjectEmpty(itemDiff!.deleted))
            audit.auditItem(ChangeType.UPDATE, item.id, item.identifier, itemDiff!, context.getCurrentUser()!.login, item.updatedAt)
        }
      }

      return result
    },

    executeAction: async (parent: any, { itemId, actionIdentifier, data }: any, context: Context) => {
      context.checkAuth()

      const nId = parseInt(itemId)
      const item = await Item.applyScope(context).findByPk(nId)
      if (!item) throw new Error('Failed to find item by id: ' + nId + ', tenant: ' + context.getCurrentUser()!.tenantId)

      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)
      const action = mng.getActions().find(elem => elem.identifier === actionIdentifier)
      if (!action) throw new Error('Failed to find action by identifier: ' + actionIdentifier + ', tenant: ' + context.getCurrentUser()!.tenantId)

      const { channels, values, result } = await processItemButtonActions2(context, [action], item, data, '')

      if (!context.canEditItem(item)) return result

      let itemDiff: AuditItem
      if (audit.auditEnabled()) itemDiff = diff({ values: item.values }, { values: values })

      item.values = mergeValues(values, item.values)
      item.changed("values", true)
      item.channels = channels

      item.updatedBy = context.getCurrentUser()!.login
      await sequelize.transaction(async (t) => {
        await item.save({ transaction: t })
      })

      if (audit.auditEnabled()) {
        if (!isObjectEmpty(itemDiff!.added) || !isObjectEmpty(itemDiff!.changed) || !isObjectEmpty(itemDiff!.deleted))
          audit.auditItem(ChangeType.UPDATE, item.id, item.identifier, itemDiff!, context.getCurrentUser()!.login, item.updatedAt)
      }

      return result
    },

    testAction: async (parent: any, { itemId, actionId }: any, context: Context) => {
      context.checkAuth()

      const nId = parseInt(itemId)
      const item = await Item.applyScope(context).findByPk(nId)
      if (!item) throw new Error('Failed to find item by id: ' + nId + ', tenant: ' + context.getCurrentUser()!.tenantId)

      if (!context.canEditItem(item)) {
        throw new Error('User :' + context.getCurrentUser()?.login + ' can not edit item :' + item.id + ', tenant: ' + context.getCurrentUser()!.tenantId)
      }

      const cId = parseInt(actionId)
      const mng = ModelsManager.getInstance().getModelManager(context.getCurrentUser()!.tenantId)

      const act = mng.getActions().find(act => act.id === cId)
      if (!act) throw new Error('Failed to find action by id: ' + cId + ', tenant: ' + mng.getTenantId())

      try {
        const { values, log, compileError, message, error } = await testAction(context, act, item)

        let itemDiff: AuditItem
        if (audit.auditEnabled()) itemDiff = diff({ values: item.values }, { values: values })

        item.values = values

        item.updatedBy = context.getCurrentUser()!.login
        await sequelize.transaction(async (t) => {
          await item.save({ transaction: t })
        })

        if (audit.auditEnabled()) {
          if (!isObjectEmpty(itemDiff!.added) || !isObjectEmpty(itemDiff!.changed) || !isObjectEmpty(itemDiff!.deleted))
            audit.auditItem(ChangeType.UPDATE, item.id, item.identifier, itemDiff!, context.getCurrentUser()!.login, item.updatedAt)
        }

        return { failed: compileError ? true : false, log, error, message, compileError: compileError || '' }
      } catch (error: any) {
        return { failed: true, log: '', error: error.message, compileError: '' }
      }
    }
  }
}
