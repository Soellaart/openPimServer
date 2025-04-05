/**
 * WBNewChannelHandler class processes items to synchronize them with Wildberries.
 * It handles:
 *  - Checking channel configuration (WB token, attribute references).
 *  - Fetching and updating items.
 *  - Sending and retrieving data from Wildberries (including error handling and logging).
 *  - Synchronizing item information (like product codes, barcodes, and attributes) with Wildberries.
 *  - Uploading and updating images, plus additional data (like rating) from external WB endpoints.
 */

import { Channel, ChannelExecution } from '../../models/channels'
import { ChannelAttribute, ChannelCategory, ChannelHandler } from '../ChannelHandler'
import fetch from 'node-fetch'
const FormData = require('form-data')
import NodeCache = require('node-cache')
import { Item } from '../../models/items'
import logger from "../../logger"
import { sequelize } from '../../models'
import * as uuid from "uuid"
import * as fs from 'fs'
import { Op } from 'sequelize'
import { ItemRelation } from '../../models/itemRelations'
import { request } from 'http'
import { ModelManager } from '../../models/manager'

interface JobContext {
    log: string
    variantParent?: string
    variantRequest?: any
    variantItems?: [Item]
}

export class WBNewChannelHandler extends ChannelHandler {
    private cache = new NodeCache({ useClones: false });

    public async processChannel(channel: Channel, language: string, data: any): Promise<void> {
        
        const chanExec = await this.createExecution(channel)

        const context: JobContext = {log: ''}

        if (!channel.config.wbToken) {
            await this.finishExecution(channel, chanExec, 3, 'API token not provided in channel configuration')
            return
        }

        if (!channel.config.imtIDAttr) {
            await this.finishExecution(channel, chanExec, 3, 'Attribute to store imtID not provided')
            return
        }

        if (!channel.config.nmIDAttr) {
            await this.finishExecution(channel, chanExec, 3, 'Attribute to store nmID not provided')
            return
        }

        if (!channel.config.wbCodeAttr) {
            await this.finishExecution(channel, chanExec, 3, 'Attribute containing product article not provided')
            return
        }
        try {
            if (!data) {
                const query: any = {}
                query[channel.identifier] = { status: 1 }
                let items = await Item.findAndCountAll({ 
                    where: { tenantId: channel.tenantId, channels: query },
                    order: [['parentIdentifier', 'ASC'], ['id', 'ASC']]
                })
                context.log += 'Found ' + items.count + ' records to process \n\n'
                for (let i = 0; i < items.rows.length; i++) {
                    const item = items.rows[i];
                    await this.processItem(channel, item, language, context)
                    context.log += '\n\n'
                }
            } else if (data.sync) {
                await this.syncJob(channel, context, data, language)
            }

            await this.finishExecution(channel, chanExec, 2, context.log)
        } catch (err) {
            logger.error("Error on channel processing", err)
            context.log += 'Channel run error - ' + JSON.stringify(err)
            await this.finishExecution(channel, chanExec, 3, context.log)
        }
    }

    async syncJob(channel: Channel, context: JobContext, data: any, language: string) {
        context.log += 'WB synchronization started\n'

        const wbCodeAttr = channel.config.wbCodeAttr
        if (!wbCodeAttr) {
            context.log += 'Error, product article attribute not set in channel configuration\n'
            return 
        }

        let singleItem = null
        if (data.item) singleItem = await Item.findByPk(data.item)

        const errorsResp = await fetch('https://suppliers-api.wildberries.ru/content/v2/cards/error/list', {
            headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
        })
        const errorsJson = await errorsResp.json()
        let msg = "Found " + errorsJson.data.length + " errors"
        logger.info(msg)
        if (channel.config.debug) context.log += msg + '\n'
        for (let i = 0; i < errorsJson.data.length; i++) {
            const error = errorsJson.data[i];

            if (singleItem && singleItem.values[channel.config.wbCodeAttr] != error.vendorCode) continue
            
            const query: any = {}
            query[wbCodeAttr] = error.vendorCode
            let item = await Item.findOne({ 
                where: { 
                    tenantId: channel.tenantId, 
                    values: query, 
                    [Op.or] : channel.visible.map((parentId: any) => {
                        return { path: { [Op.regexp]: '*.' + parentId + '.*' } }
                    })
                } 
            })
            if (!item) {
                let msg = "Error, no item found by article for synchronization: " + error.vendorCode
                logger.info(msg)
                context.log += msg + '\n'
            } else {
                if (item.channels[channel.identifier]) {
                    item.channels[channel.identifier].status = 3
                    item.channels[channel.identifier].wbError = true
                    item.channels[channel.identifier].message = error.errors.join(', ')
                    data.syncedAt = Date.now()
                    item.changed('channels', true)
                    item.save()
                    let msg = "Error, for product: " + error.vendorCode + ", " + item.channels[channel.identifier].message
                    logger.info(msg)
                    context.log += msg + '\n'
                }
            }
        }

        if (data.item) {
            if (singleItem) {
                await this.syncItems(channel, [singleItem], context, true, language)
            }
        } else {
            const query: any = {}
            query[channel.identifier] = { status: { [Op.ne]: null } }
            let items = await Item.findAll({ 
                where: { tenantId: channel.tenantId, channels: query } 
            })
            context.log += 'Found ' + items.length + ' records to process \n\n'
            await this.syncItems(channel, items, context, false, language)
        }
        context.log += 'Synchronization finished'
    }

    async syncItems(channel: Channel, items: Item[], context: JobContext, singleSync: boolean, language: string) {
        if (singleSync) {
            const item = items[0]
            context.log += 'Processing product with identifier: [' + item.identifier + ']\n'
            
            if (item.channels[channel.identifier]) {
                const chanData = item.channels[channel.identifier]
                if (chanData && chanData.status === 3 && !item.channels[channel.identifier]?.wbError) {
                    // If the status is error, synchronization won't run
                    context.log += 'Product status - error, synchronization not performed \n'
                    return
                }
    
                const article = item.values[channel.config.wbCodeAttr]
                if (!article) {
                    item.channels[channel.identifier].status = 3
                    item.channels[channel.identifier].wbError = false
                    item.channels[channel.identifier].message = 'No product article value found in attribute: ' + channel.config.wbCodeAttr
                } else {
                    const url = 'https://suppliers-api.wildberries.ru/content/v2/get/cards/list'
                    const request = {	
                        settings: {
                            filter: {
                                withPhoto: -1,
                                textSearch: '' + article
                            }
                        }
                    }
        
                    const serverConfig = ModelManager.getServerConfig()
                    if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)
    
                    let msg = "Request to WB: " + url + " => " + JSON.stringify(request)
                    logger.info(msg)
                    if (channel.config.debug) context.log += msg + '\n'
                    const res = await fetch(url, {
                        method: 'post',
                        body:    JSON.stringify(request),
                        headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
                    })
        
                    if (res.status !== 200) {
                        const msg = 'Wildberries request error: ' + res.statusText
                        context.log += msg
                        return
                    } else {
                        const json = await res.json()
                        if (json.cards.length === 0) {
                            const msg = 'No results found for this request'
                            logger.info(msg)
                            context.log += msg
                            return
                        }

                        await this.processItemSync(channel, items, context, language, json)
                    }
                }
            } else {
                context.log += '  product with identifier ' + item.identifier + ' does not require synchronization \n'
            }
        } else {
            let pageSize = 0
            const limit = 100
            let updatedAt
            let nmID
            do {
                let cursor: any = { limit: limit }
                if (updatedAt && nmID) cursor = { limit: limit, updatedAt: updatedAt, nmID: nmID }
                const url = 'https://suppliers-api.wildberries.ru/content/v2/get/cards/list'
                const request = {
                    settings: {
                        cursor: cursor,
                        filter: {
                            withPhoto: -1
                        }
                    }
                }

                const serverConfig = ModelManager.getServerConfig()
                if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)

                let msg = "Request to WB: " + url + " => " + JSON.stringify(request)
                logger.info(msg)
                if (channel.config.debug) context.log += msg + '\n'
                const res = await fetch(url, {
                    method: 'post',
                    body: JSON.stringify(request),
                    headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
                })

                if (res.status !== 200) {
                    const msg = 'Wildberries request error: ' + res.statusText
                    logger.info(msg)
                    context.log += msg
                    return
                } else {
                    const json = await res.json()
                    if (json.cards.length === 0) {
                        const msg = 'No results found for this request'
                        logger.info(msg)
                        context.log += msg
                        return
                    }

                    await this.processItemSync(channel, items, context, language, json)

                    pageSize = json.cursor.total
                    updatedAt = json.cursor.updatedAt
                    nmID = json.cursor.nmID
                }
            } while (pageSize === limit)
        }
    }

    async processItemSync(channel: Channel, items: Item[], context: JobContext, language: string, json: any) {
        let msg = `Received ${json.cards.length} cards\n`
        if (channel.config.debug) context.log += msg
        logger.info(msg)

        for (const card of json.cards) {
            const item = items.find(elem => elem.values[channel.config.wbCodeAttr] == card.vendorCode)
            if (card.imtID) {
                msg = `Processing card: nmID: ${card.nmID}, imtID: ${card.imtID}, vendorCode: ${card.vendorCode}\n`
                if (channel.config.debug) context.log += msg
                logger.info(msg)
                if (item) {
                    msg = `Item found: ${item.identifier}\n`
                    if (channel.config.debug) context.log += msg
                    logger.info(msg)

                    if (!item.channels[channel.identifier]) {
                        msg = 'Product ' + item.identifier + ' has no channel export, synchronization will not be performed \n'
                        if (channel.config.debug) context.log += msg
                        logger.info(msg)
                        continue
                    }

                    const chanData = item.channels[channel.identifier]
                    if (chanData && chanData.status === 3 && !item.channels[channel.identifier]?.wbError) {
                        // If the status is error, synchronization won't run
                        msg = 'Product status - error, synchronization not performed \n'
                        if (channel.config.debug) context.log += msg
                        logger.info(msg)
                        continue
                    }
                    let status = 2
                    // If item was created first time (imtIDAttr is empty) send it again to WB to send images
                    // (images can be assigned only to existing items)
                    if (channel.config.imtIDAttr && !item.values[channel.config.imtIDAttr]) status = 1
                    if (item.channels[channel.identifier].status != status) {
                        item.channels[channel.identifier].status = status
                        item.channels[channel.identifier].wbError = false
                        item.channels[channel.identifier].message = ""
                        item.channels[channel.identifier].syncedAt = Date.now()
                        item.changed('channels', true)
                    }
                    msg = `Synchronization result: ${JSON.stringify(item.channels[channel.identifier])}\n`
                    if (channel.config.debug) context.log += msg
                    logger.info(msg)
                    
                    let changed = false
                    if (channel.config.imtIDAttr && !item.values[channel.config.imtIDAttr]) {
                        item.values[channel.config.imtIDAttr] = card.imtID
                        changed = true
                    }
                    if (channel.config.nmIDAttr && !item.values[channel.config.nmIDAttr]) {
                        item.values[channel.config.nmIDAttr] = card.nmID
                        changed = true
                    }
                    if (channel.config.wbBarcodeAttr) {
                        const categoryConfig = await this.getCategoryConfig(channel, item)
                        if (categoryConfig) {
                            const barcodeConfig = categoryConfig.attributes.find((elem: any) => elem.id === '#barcode')
                            const barcode = await this.getValueByMapping(channel, barcodeConfig, item, language)

                            if (!barcode && !item.values[channel.config.wbBarcodeAttr]) {
                                item.values[channel.config.wbBarcodeAttr] = card.sizes[0].skus[0]
                                changed = true
                            }
                        }
                    }

                    if (channel.config.wbAttrContentRating && channel.config.wbGetContentRating && item.values[channel.config.nmIDAttr]) {
                        const nmId = item.values[channel.config.nmIDAttr]
                        const urlRating = `https://feedbacks-api.wildberries.ru/api/v1/feedbacks/products/rating/nmid?nmId=${nmId}`
                        const logRating = "Sending request to WB: " + urlRating + " => nmId = " + JSON.stringify(nmId)
                        logger.info(logRating)
                        if (channel.config.debug) context.log += logRating + '\n'

                        const serverConfig = ModelManager.getServerConfig()
                        if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)

                        const resRating = await fetch(urlRating, {
                            method: 'get',
                            headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken }
                        })

                        if (resRating.status !== 200) {
                            const msg = 'WB request error when obtaining average product rating: ' + resRating.statusText
                            context.log += msg + '\n'
                            logger.error(msg)
                        } else {
                            const dataRating = await resRating.json()
                            logger.info('Received data: ' + JSON.stringify(dataRating))
                            if (channel.config.debug) context.log += 'Received data: ' + JSON.stringify(dataRating) + '\n'
                            context.log += 'Product with identifier ' + item.identifier + ' is being processed\n'

                            if (!item.values[channel.config.wbAttrContentRating]) {
                                item.values[channel.config.wbAttrContentRating] = '' + dataRating.data?.valuation
                                changed = true
                            }
                        }
                    }
                    if (changed) item.changed('values', true)

                    await sequelize.transaction(async (t) => {
                        await item.save({ transaction: t })
                    })
                } else {
                    let msg = "Error, no item found by article for synchronization: " + card.vendorCode
                    context.log += msg + '\n'
                }
            } else {
                context.log += 'No new data received\n'
            }
        }
    }

    async getCategoryConfig(channel: Channel, item: Item) {
        for (const categoryId in channel.mappings) {
            const categoryConfig = channel.mappings[categoryId]

            if (categoryConfig.valid && categoryConfig.valid.length > 0 && (
                (categoryConfig.visible && categoryConfig.visible.length > 0) || categoryConfig.categoryExpr || (categoryConfig.categoryAttr && categoryConfig.categoryAttrValue)) ) {
                const pathArr = item.path.split('.')
                const tstType = categoryConfig.valid.includes(item.typeId) || categoryConfig.valid.includes('' + item.typeId)
                if (tstType) {
                    let tst = null
                    if (categoryConfig.visible && categoryConfig.visible.length > 0) {
                        if (categoryConfig.visibleRelation) {
                            let sources = await Item.findAll({
                                where: { tenantId: channel.tenantId, '$sourceRelation.relationId$': categoryConfig.visibleRelation, '$sourceRelation.targetId$': item.id },
                                include: [{ model: ItemRelation, as: 'sourceRelation' }]
                            })
                            tst = sources.some(source => {
                                const pathArr = source.path.split('.')
                                return categoryConfig.visible.find((elem: any) => pathArr.includes('' + elem))
                            })
                        } else {
                            tst = categoryConfig.visible.find((elem: any) => pathArr.includes('' + elem))
                        }
                    } else if (categoryConfig.categoryExpr) {
                        tst = await this.evaluateExpression(channel, item, categoryConfig.categoryExpr)
                    } else {
                        tst = item.values[categoryConfig.categoryAttr] && item.values[categoryConfig.categoryAttr] == categoryConfig.categoryAttrValue
                    }
                    if (tst) {
                        return categoryConfig
                    }
                }
            }
        }
        return null
    }

    async processItem(channel: Channel, item: Item, language: string, context: JobContext) {
        context.log += 'Processing record with identifier: ' + item.identifier + '\n'
        const categoryConfig = await this.getCategoryConfig(channel, item)
        if (categoryConfig) {
            try {
                await this.processItemInCategory(channel, item, categoryConfig, language, context)
                await sequelize.transaction(async (t) => {
                    await item.save({ transaction: t })
                })

                // await new Promise(resolve => setTimeout(resolve, 5000))
            } catch (err) {
                logger.error("Failed to process item with id: " + item.id + " for tenant: " + item.tenantId, err)
            }
        } else {
            context.log += 'Record with identifier: ' + item.identifier + ' does not match the channel configuration.\n'
            const data = item.channels[channel.identifier]
            data.status = 3
            data.wbError = false
            data.message = 'This object does not match any category in this channel.'
            context.log += 'Record with identifier:' + item.identifier + ' does not match any category in this channel.\n'
            item.changed('channels', true)
            await sequelize.transaction(async (t) => {
                await item.save({ transaction: t })
            })
        }
    }

    async processItemInCategory(channel: Channel, item: Item, categoryConfig: any, language: string, context: JobContext) {
        context.log += 'Category "' + categoryConfig.name + '" found for record with identifier: ' + item.identifier + '\n'

        const data = item.channels[channel.identifier]
        data.category = categoryConfig.id

        const productCodeConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#productCode')
        let productCode = await this.getValueByMapping(channel, productCodeConfig, item, language)
        if (!productCode) {
            const msg = 'No configuration provided for "Product Article" for category: ' + categoryConfig.name
            context.log += msg
            this.reportError(channel, item, msg)
            return
        }
        productCode = ''+productCode

        const barcodeConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#barcode')
        const barcode = await this.getValueByMapping(channel, barcodeConfig, item, language)
        if (!barcode && !channel.config.barcodeBool) {
            const msg = 'No configuration provided for "Barcode" for category: ' + categoryConfig.name
            context.log += msg
            this.reportError(channel, item, msg)
            return
        }

        const titleConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#title')
        const title = await this.getValueByMapping(channel, titleConfig, item, language)

        const descriptionConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#description')
        const description = await this.getValueByMapping(channel, descriptionConfig, item, language)

        const brandConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#brand')
        const brand = await this.getValueByMapping(channel, brandConfig, item, language)

        const lengthConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#length')
        const length = await this.getValueByMapping(channel, lengthConfig, item, language)

        const widthConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#width')
        const width = await this.getValueByMapping(channel, widthConfig, item, language)

        const heightConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#height')
        const height = await this.getValueByMapping(channel, heightConfig, item, language)

        const serverConfig = ModelManager.getServerConfig()

        const nmID = item.values[channel.config.nmIDAttr]
        const priceConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#price')
        const price = await this.getValueByMapping(channel, priceConfig, item, language)
        if (!nmID && !price) {
            // price is necessary only for creation
            const msg = 'No configuration provided for "Price" for category: ' + categoryConfig.name
            context.log += msg
            this.reportError(channel, item, msg)
            return
        }

        // request to WB
        let request: any = {
            vendorCode: productCode,
            dimensions: { length: length || 0, width: width || 0, height: height || 0 },
            characteristics: [],
            sizes: [{ wbSize: "", price: price, skus: barcode ? (Array.isArray(barcode) ? barcode : ['' + barcode]) : [] }]
        }

        if (nmID) {
            const existUrl = 'https://suppliers-api.wildberries.ru/content/v2/get/cards/list'
            const existsBody = {
                settings: {
                    filter: {
                        withPhoto: -1,
                        textSearch: productCode
                    }
                }
            }

            if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)
            let msg = "Sending request to Wildberries: " + existUrl + " => " + JSON.stringify(existsBody)
            logger.info(msg)
            const resExisting = await fetch(existUrl, {
                method: 'post',
                body:    JSON.stringify(existsBody),
                headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
            })
            if (resExisting.status !== 200) {
                const msg = 'Wildberries request error - https://suppliers-api.wildberries.ru/content/v2/get/cards/list: ' + resExisting.statusText
                context.log += msg
                const data = this.reportError(channel, item, msg)
                data.wbError = true
                return
            } else {
                const json = await resExisting.json()
                // if (channel.config.debug) context.log += 'received response (load existing data):'+JSON.stringify(json)+'\n'
                const tst = json.cards.find((elem:any) => elem.nmID == nmID)
                if (tst) {
                    request = tst
                } else {
                    logger.warn('Failed to find existing product by code: ' + productCode)
                }
                request.vendorCode = productCode
                request.dimensions.length = length || 0
                request.dimensions.width = width || 0
                request.dimensions.height = height || 0
                request.sizes[0].skus = barcode ? (Array.isArray(barcode) ? barcode : ['' + barcode]) : []
            }

            request.nmID = parseInt(nmID)
        }

        if (title) request.title = title
        if (description) request.description = description
        if (brand) request.brand = brand

        // attributes
        const create = item.values[channel.config.imtIDAttr] ? false : true
        for (let i = 0; i < categoryConfig.attributes.length; i++) {
            const attrConfig = categoryConfig.attributes[i];

            if (!attrConfig.id.startsWith('#')) {
                const attr = (await this.getAttributes(channel, categoryConfig.id)).find(elem => elem.id === attrConfig.id)
                if (!attr) {
                    logger.warn('Failed to find attribute in channel for attribute with id: ' + attrConfig.id)
                    continue
                }
                try {
                    const value = await this.getValueByMapping(channel, attrConfig, item, language)
                    if (!create) {
                        if (!request.characteristics) request.characteristics = []
                        this.clearPreviousValue(request.characteristics, attr.type)
                    }
                    if (value) {
                        const data: any = {}
                        data.id = attr.type
                        if (Array.isArray(value) && value.length > 0) {
                            data.value = attr.maxCount == 0 || attr.maxCount == 1 ? value[0] : value
                        } else {
                            data.value = attr.maxCount == 0 || attr.maxCount == 1 ? value : [value]
                        }
                        request.characteristics.push(data)
                    } else if (attr.required) {
                        const msg = 'No value for required attribute "' + attr.name + '" for category: ' + categoryConfig.name
                        context.log += msg
                        this.reportError(channel, item, msg)
                        return
                    }
                } catch (err: any) {
                    const msg = 'Error evaluating attribute "' + attr.name + '" for category: ' + categoryConfig.name
                    logger.error(msg, err)
                    context.log += msg + ': ' + err.message
                    this.reportError(channel, item, msg + ': ' + err.message)
                    return
                }
            }
        }

        if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)
        const tst = await this.sendRequest(channel, item, request, context, categoryConfig.id)

        // images
        if (!create && tst) {
            // Send images on update only; product is not available in WB right after create
            if (!nmID) {
                let msg = "nmID is empty so images will not be sent"
                logger.info(msg)
                if (channel.config.debug) context.log += msg + '\n'
            } else {
                const imageConfig = categoryConfig.attributes.find((elem:any) => elem.id === '#images')
                if (!imageConfig) return
                const images = await this.getValueByMapping(channel, imageConfig, item, language)
                if (images && images.length > 0) {
                    const imgRequest = {
                        "nmId": parseInt(nmID),
                        "data": images
                    }
                    if (serverConfig.wbRequestDelay) await this.sleep(serverConfig.wbRequestDelay)
                    const imgUrl = 'https://suppliers-api.wildberries.ru/content/v3/media/save'
                    let msg = "Sending request to Wildberries: " + imgUrl + " => " + JSON.stringify(imgRequest)
                    logger.info(msg)
                    if (channel.config.debug) context.log += msg + '\n'

                    if (process.env.OPENPIM_WB_EMULATION === 'true') {
                        const msg = 'Emulation enabled, message not sent to WB'
                        if (channel.config.debug) context.log += msg + '\n'
                        logger.info(msg)
                        return
                    }

                    const res = await fetch(imgUrl, {
                        method: 'post',
                        body:    JSON.stringify(imgRequest),
                        headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
                    })
                    msg = "Response status from Wildberries: " + res.status
                    logger.info(msg)
                    if (channel.config.debug) context.log += msg + '\n'
                    if (res.status !== 200) {
                        const msg = 'Wildberries request error: ' + (await res.text())
                        context.log += msg
                        const data = this.reportError(channel, item, msg)
                        data.wbError = true
                        return
                    }
                }
            }
        }
    }

    private clearPreviousValue(arr: any[], type: string) {
        if (!arr) return
        const tst = arr.findIndex((elem:any) => elem.id == type)
        if (tst != -1) arr.splice(tst, 1)
    }

    async sendRequest(channel: Channel, item: Item, request: any, context: JobContext, categoryId: string) {
        const create = item.values[channel.config.nmIDAttr] ? false : true

        let grpItem = null
        if (channel.config.wbGroupAttr) {
            const grp = item.values[channel.config.wbGroupAttr]
            if (grp) {
                // find all items in this group
                const query: any = {}
                query[channel.config.wbGroupAttr] = grp
                let grpItems = await Item.findAll({ where: { tenantId: channel.tenantId, values: query, id: { [Op.ne]: item.id } } })
                // find item already at WB
                grpItem = grpItems.find(elem => elem.values[channel.config.imtIDAttr])
                // find item already send to WB but without imtID yet
                if (!grpItem) grpItem = grpItems.find(elem => elem.channels[channel.identifier]?.status === 4 )
            }
        }

        let url = ''
        let req

        const idx = categoryId.indexOf('-')
        const objId = parseInt(categoryId.substring(idx + 1))

        if (grpItem) {
            url = create ? 'https://suppliers-api.wildberries.ru/content/v2/cards/upload/add' : 'https://suppliers-api.wildberries.ru/content/v2/cards/update'
            req = create ? { imtID: item.values[channel.config.imtIDAttr], cardsToAdd: [request] } : [request]
        } else {
            url = create ? 'https://suppliers-api.wildberries.ru/content/v2/cards/upload' : 'https://suppliers-api.wildberries.ru/content/v2/cards/update'
            req = create ? [{ subjectID: objId, variants: [request] }] : [request]
        }

        let msg = "Sending request to Wildberries: " + url + " => " + JSON.stringify(req)
        logger.info(msg)
        if (channel.config.debug) context.log += msg + '\n'

        if (process.env.OPENPIM_WB_EMULATION === 'true') {
            const msg = 'Emulation enabled, message not sent to WB'
            if (channel.config.debug) context.log += msg + '\n'
            logger.info(msg)
            return true
        }

        const res = await fetch(url, {
            method: 'post',
            body:    JSON.stringify(req),
            headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
        })
        msg = "Response status from Wildberries: " + res.status
        logger.info(msg)
        if (channel.config.debug) context.log += msg + '\n'
        if (res.status !== 200) {
            const msg = 'Wildberries request error: ' + (await res.text())
            context.log += msg
            const data = this.reportError(channel, item, msg)
            data.wbError = true
            return false
        } else {
            const json = await res.json()
            if (channel.config.debug) context.log += 'Received response:' + JSON.stringify(json) + '\n'
            if (json.error) {
                const msg = 'Wildberries request error: ' + json.error.message
                const data = this.reportError(channel, item, msg)
                data.wbError = true
                context.log += msg
                logger.info("Error from Wildberries: " + JSON.stringify(json))
                return false
            } else {
                context.log += 'Record with identifier: ' + item.identifier + ' processed successfully.\n'
                const data = item.channels[channel.identifier]
                data.status = 4
                data.wbError = false
                data.message = ''
                data.syncedAt = Date.now()
                item.changed('channels', true)
                return true
            }
        }
    }

    public async getCategories(channel: Channel): Promise<{ list: ChannelCategory[] | null, tree: ChannelCategory | null }> {
        let tree: ChannelCategory | undefined = this.cache.get('categories')
        if (!tree) {
            const url = 'https://suppliers-api.wildberries.ru/content/v2/object/parent/all'
            logger.info("Sending GET request to WB: " + url)
            const res = await fetch(url, {
                method: 'get',
                headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
            })
            const json = await res.json()
            if (channel.config.debug) logger.info('WB response: ' + JSON.stringify(json))
            const data: ChannelCategory[] = Object.values(json.data).map((value: any) => {
                return { id: 'cat_' + value.id, name: value.name, children: [] }
            })

            let offset = 0
            let length = 0
            do {
                const url2 = 'https://suppliers-api.wildberries.ru/content/v2/object/all?limit=1000&offset=' + offset
                logger.info("Sending GET request to WB: " + url2)
                const res2 = await fetch(url2, {
                    method: 'get',
                    headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
                })
                const json2 = await res2.json()
                if (channel.config.debug) logger.info('WB response2: ' + JSON.stringify(json2))

                for(const obj of json2.data) {
                    const cat = 'cat_' + obj.parentID
                    const parent = data.find(elem => elem.id === cat)
                    if (!parent) {
                        logger.warning(`Failed to find parent for ${JSON.stringify(obj)}`)
                    } else {
                        parent.children!.push({ id: parent.id + '-' + obj.subjectID, name: obj.subjectName, children: [] })
                    }
                }

                length = json2.data.length
                offset += 1000
            } while (length > 0)

            tree  = { id: '', name: 'root', children: data.filter(elem => elem.children!.length > 0) }
            this.cache.set('categories', tree, 3600)
        }
        return { list: null, tree: tree }
    }

    public async getAttributes(channel: Channel, categoryId: string): Promise<ChannelAttribute[]> {
        let data = this.cache.get('attr_' + categoryId)
            if (!data) {
            const idx = categoryId.indexOf('-')
            const objId = categoryId.substring(idx + 1)

            const res = await fetch('https://suppliers-api.wildberries.ru/content/v2/object/charcs/' + objId, {
                method: 'get',
                body:    JSON.stringify(request),
                headers: { 'Content-Type': 'application/json', 'Authorization': channel.config.wbToken },
            })
            const json = await res.json()
            data = Object.values(json.data).map((data: any) => {
                return {
                    id: 'wbattr_' + data.charcID,
                    type: data.charcID,
                    description: 'id: ' + data.charcID + ', category: ' + objId,
                    isNumber: data.charcType === 1 || data.charcType === 0 ? false : true,
                    name: data.name + (data.unitName ? ' (' + data.unitName + ')' : '') + (data.charcType === 4 ? ' [number]' : ''),
                    category: categoryId,
                    required: data.required,
                    maxCount: data.maxCount,
                    dictionary: false
                    // dictionaryLink could be included if needed
                }
            })

            this.cache.set('attr_' + categoryId, data, 3600)
        }
        return <ChannelAttribute[]>data
    }
}
