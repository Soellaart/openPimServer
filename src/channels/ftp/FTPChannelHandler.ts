import SftpClient from 'ssh2-sftp-client';
import { ChannelHandler, ChannelCategory, ChannelAttribute } from '../ChannelHandler';
import { Channel, ChannelExecution } from '../../models/channels';
import { Item } from '../../models/items';
import logger from '../../logger';
import * as fs from 'fs';
import * as path from 'path';
import { sequelize } from '../../models';
import { Op } from 'sequelize';

interface FTPJobContext {
  log: string;
}

export class FTPChannelHandler extends ChannelHandler {
  private sftpClient = new SftpClient();

  /**
   * Entry point for processing the FTP channel. Decides whether to generate (upload) or download CSV.
   */
  public async processChannel(channel: Channel, language: string, data: any): Promise<void> {
    const chanExec = await this.createExecution(channel);
    const context: FTPJobContext = { log: '' };

    if (!channel.config.ftpHost) {
      await this.finishExecution(channel, chanExec, 3, 'SFTP host not provided');
      return;
    }
    if (!channel.config.ftpUser) {
      await this.finishExecution(channel, chanExec, 3, 'SFTP username not provided');
      return;
    }
    if (!channel.config.ftpPassword) {
      await this.finishExecution(channel, chanExec, 3, 'SFTP password not provided');
      return;
    }

    logger.info("Starting FTP channel processing", { channelId: channel.id });

    const hasHeaders = channel.headerMappings && Object.keys(channel.headerMappings).length > 0;
    // Check if headerMapping is defined if not the headers sould be the same so we can still continue
    if (!hasHeaders) {
      context.log += 'No headerMapping defined, using default headers.\n';
    } else {
      context.log += 'Header mapping defined, using custom headers.\n';
    }

    try {

        await this.processCSV(channel, context, await this.downloadCSV(channel, context), language);
      logger.info("FTP channel processing complete", { channelId: channel.id });
      await this.finishExecution(channel, chanExec, 2, context.log);
    } catch (err: any) {
      logger.error('Error in FTP channel processing', err);
      context.log += 'Error running channel - ' + (err.message || err);
      await this.finishExecution(channel, chanExec, 3, context.log);
    }
  }

  /**
   * Generate a CSV from local items, then upload via SFTP using ssh2-sftp-client.
   */
  private async downloadCSV(channel: Channel, context: FTPJobContext): Promise<string> {
    let constLocalFilePath = '/TEMP/FTP/';
    if (!fs.existsSync(constLocalFilePath)) {
      fs.mkdirSync(constLocalFilePath, { recursive: true });
    }
    context.log += 'Downloading CSV from SFTP...\n';
    logger.info(JSON.stringify(channel));
    const remoteDir = channel.config.ftpRemoteDir || '/';
    const remoteFilePath = path.posix.join(remoteDir, channel.config.remoteFilename || 'test.csv');
    const localFilePath = path.join("/TEMP/FTP/", `import_${Date.now()}.csv`);

    try {
      await this.sftpClient.connect({
        host: channel.config.ftpHost,
        port: channel.config.ftpPort || 22,
        username: channel.config.ftpUser,
        password: channel.config.ftpPassword,
      });

      await this.sftpClient.fastGet(remoteFilePath, localFilePath );
      logger.info(`CSV downloaded from SFTP to ${localFilePath}`);
      context.log += `CSV downloaded from SFTP to ${localFilePath}.\n`;
    } catch (sftpErr: any) {
      context.log += `SFTP download error: ${sftpErr.message}\n`;
      logger.error('SFTP download error', sftpErr);
    } finally {
      await this.sftpClient.end();
    }

    if (!fs.existsSync(localFilePath)) {
      context.log += 'Downloaded file not found locally, abort.\n';
    }
    return localFilePath;
  }

  private async processCSV(channel: Channel, context: FTPJobContext, localFilePath: string, language: string) {
    const csvContent = fs.readFileSync(localFilePath, 'utf8');
    if (csvContent) {}

    const lines = csvContent
      .split('\n')
      .map((l) => l.trim())
      .filter(Boolean);
    if (lines.length < 2) {
      context.log += 'CSV empty or missing data.\n';
      fs.unlinkSync(localFilePath);
      return;
    }

    const headers = lines[0].split(',');
    context.log += `CSV headers: ${headers.join(', ')}\n`;

    for (let i = 1; i < lines.length; i++) {
      const rowVals = lines[i].split(',');
      const rowData: { [csvHeader: string]: any } = {};
      for (let j = 0; j < headers.length; j++) {
        const Header = headers[j].replace(/^"+|"+$/g, '').trim();
        rowData[Header] = rowVals[j] ? rowVals[j].replace(/^"+|"+$/g, '').trim() : '';
      }

      const localAttrs = this.mapExternalRowToLocalAttributes(channel, rowData);

      const codeField = channel.dataIdentifier;
      if (codeField && localAttrs[codeField]) {
        const sku = localAttrs[codeField];
        const item = await Item.findOne({
          where: {
            tenantId: channel.tenantId,
            [`values.${codeField}`]: sku,
          },
        });

        if (!item) {
          context.log += `No item found for SKU [${sku}], row ${i}.\n`;
        } else {
          for (const key in localAttrs) {
            item.values[key] = localAttrs[key];
          }

          item.channels[channel.identifier] = item.channels[channel.identifier] || {};
          item.channels[channel.identifier].status = 2;
          item.channels[channel.identifier].syncedAt = Date.now();
          item.changed('values', true);
          item.changed('channels', true);

          await sequelize.transaction(async (t) => {
            await item.save({ transaction: t });
          });
          context.log += `Updated item ${item.identifier} from row ${i}.\n`;
        }
      } else {
        context.log += `Row ${i}: missing SKU or item not found.\n`;
      }
    }

    fs.unlinkSync(localFilePath);
    context.log += 'Import CSV processing complete.\n';
  }

  public async testConnection(config: any): Promise<{ success: boolean; message: string; headers?: string[] }> {
    const connectionResult = await this.ftpConnection(config);
    if (!connectionResult.success) {
      return { success: false, message: `Connection failed: ${connectionResult.message}` };
    }

    const fileExists = await this.checkFileExists(config.ftpRemoteDir + config.remoteFilename || 'test.csv', config);
    if (!fileExists) {
      return { success: false, message: 'TestedConnection' }; //Connection successful but file does not exist.
    }

    const returnHeaders: {
      success: boolean;
      headers?: string[];
    } = await this.getHeaders(config.ftpRemoteDir + config.remoteFilename || 'test.csv', config);
    if (!returnHeaders.success) {
      return { success: false, message: 'TestedHeadersFails', headers: returnHeaders.headers }; // Connection successful but file does not have headers.
    }
    if (!returnHeaders.headers || returnHeaders.headers.length === 0) {
      return { success: false, message: 'TestedHeadersEmpty' }; //Connection successful but file has no headers.
    }

    return { success: true, message: 'TestedHeaders', headers: returnHeaders.headers }; //Connection successful and file exists with valid headers.
  }

  public async testConnection_getHeader(channelID: number): Promise<{ success: boolean; message: string; headers?: string[] }> {
    const channel = await Channel.findOne({ where: { id: channelID, active: true } });
    const config = channel ? channel.config : null;
    const connectionResult = await this.ftpConnection(config);
    if (!connectionResult.success) {
      return { success: false, message: `Connection failed: ${connectionResult.message}` };
    }

    const fileExists = await this.checkFileExists(config.ftpRemoteDir + config.remoteFilename || 'test.csv', config);
    if (!fileExists) {
      return { success: false, message: 'TestedConnection' }; //Connection successful but file does not exist.
    }

    const returnHeaders: {
      success: boolean;
      headers?: string[];
    } = await this.getHeaders(config.ftpRemoteDir + config.remoteFilename || 'test.csv', config);
    if (!returnHeaders.success) {
      return { success: false, message: 'TestedHeadersFails', headers: returnHeaders.headers }; // Connection successful but file does not have headers.
    }
    if (!returnHeaders.headers || returnHeaders.headers.length === 0) {
      return { success: false, message: 'TestedHeadersEmpty' }; //Connection successful but file has no headers.
    }
    return { success: true, message: 'TestedHeaders', headers: returnHeaders.headers }; //Connection successful and file exists with valid headers.

  }

  private async ftpConnection(config: any): Promise<{ success: boolean; message?: string }> {
    try {
      await this.sftpClient.connect({
        host: config.ftpHost,
        port: config.ftpPort || 22,
        username: config.ftpUser,
        password: config.ftpPassword,
      });
      await this.sftpClient.end();
      return { success: true };
    } catch (err: any) {
      logger.error('SFTP connection test failed', err);
      return { success: false, message: err.message };
    }
  }

  private async checkFileExists(filePath: string, config: any): Promise<boolean> {
    try {
      await this.sftpClient.connect({
        host: config.ftpHost,
        port: config.ftpPort || 22,
        username: config.ftpUser,
        password: config.ftpPassword,
      });
      const fileExists = await this.sftpClient.exists(filePath);
      await this.sftpClient.end();
      return !!fileExists;
    } catch (err: any) {
      logger.error('Error checking file existence on SFTP', err);
      return false;
    }
  }

  private async getHeaders(filePath: string, config: any): Promise<{ success: boolean; headers?: string[] }> {
    const localFilePath = path.join(__dirname, `validate_${Date.now()}.csv`);
    try {
      await this.sftpClient.connect({
        host: config.ftpHost,
        port: config.ftpPort || 22,
        username: config.ftpUser,
        password: config.ftpPassword,
      });
      await this.sftpClient.fastGet(filePath, localFilePath);
      await this.sftpClient.end();

      const csvContent = fs.readFileSync(localFilePath, 'utf8');
      const csvheaders = csvContent
        .split('\n')[0]
        .split(',')
        .map((h) => h.trim());
      fs.unlinkSync(localFilePath);
      if (csvheaders.length === 0) {
        logger.warn('CSV file has no headers');
        return { success: false, headers: [] };
      }

      return { success: true, headers: csvheaders };
    } catch (err: any) {
      logger.error('Error validating headers in CSV file', err);
      return { success: false, headers: [] };
    }
  }

  // ---------------
  // STUBS: getCategories, getAttributes
  // ---------------
  public async getCategories(channel: Channel): Promise<{
    list: ChannelCategory[] | null;
    tree: ChannelCategory | null;
  }> {
    return { list: null, tree: null };
  }

  public async getAttributes(channel: Channel, categoryId: string): Promise<ChannelAttribute[]> {
    return [];
  }
}
