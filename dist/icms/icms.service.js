"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var IcmsService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.IcmsService = void 0;
const common_1 = require("@nestjs/common");
const openquery_service_1 = require("../shared/database/openquery/openquery.service");
const prisma_service_1 = require("../prisma/prisma.service");
const xml2js = __importStar(require("xml2js"));
const zlib = __importStar(require("zlib"));
const crypto_1 = require("crypto");
const mva_data_1 = require("./constants/mva-data");
const monofasico_ncm_1 = require("./constants/monofasico-ncm");
const node_pdf_nfe_1 = require("@alexssmusica/node-pdf-nfe");
const archiver_1 = __importDefault(require("archiver"));
const stream_1 = require("stream");
let IcmsService = IcmsService_1 = class IcmsService {
    constructor(openQuery, prisma) {
        this.openQuery = openQuery;
        this.prisma = prisma;
        this.logger = new common_1.Logger(IcmsService_1.name);
        this.refData = [];
        this.monofasicoNcmSet = new Set(monofasico_ncm_1.MONOFASICO_NCM_LIST.map((ncm) => this.cleanDigits(ncm)));
        this.launchedSyncJobs = new Map();
        this.xmlNormalizationJobs = new Map();
        this.parseReferenceData();
    }
    parseReferenceData() {
        const lines = mva_data_1.CSV_DATA_CLEAN.split('\n').filter(l => l.trim() !== '');
        const headers = lines[0].split(';');
        for (let i = 1; i < lines.length; i++) {
            const parts = lines[i].split(';');
            if (parts.length < 4)
                continue;
            const row = {};
            row['Item'] = parseFloat(parts[0]);
            row['CEST'] = parts[1];
            row['NCM_SH'] = parts[2];
            row['MVA'] = parseFloat(parts[3]);
            row['Descricao'] = parts[4];
            row['NCM_CLEAN'] = row['NCM_SH'].replace(/\./g, '').trim();
            this.refData.push(row);
        }
        this.logger.log(`Loaded ${this.refData.length} reference MVA items.`);
    }
    async syncInvoices(start, end) {
        try {
            const { startDate, endDate } = this.getDateRangeOrDefault(start, end);
            const erpInvoices = await this.fetchErpInvoices(start, end);
            this.logger.log(`Fetched ${erpInvoices.length} invoices from ERP`, 'Sync');
            const erpKeys = new Set();
            const upsertTasks = [];
            for (const inv of erpInvoices) {
                erpKeys.add(inv.CHAVE_NFE);
                upsertTasks.push(async () => {
                    const normalizedXmlCompleto = await this.normalizeBlobXml(inv.XML_COMPLETO);
                    const normalizedXmlResumo = await this.normalizeBlobXml(inv.XML_RESUMO);
                    const xmlParaPersistir = normalizedXmlCompleto || normalizedXmlResumo || '';
                    const xmlParaPersistirCompactado = this.encodeXml(xmlParaPersistir);
                    let valorTotal = 0;
                    const vNfMatch = xmlParaPersistir.match(/<vNF>([\d\.]+)<\/vNF>/);
                    if (vNfMatch) {
                        valorTotal = parseFloat(vNfMatch[1]);
                    }
                    await this.prisma.nfeConciliacao.upsert({
                        where: { chave_nfe: inv.CHAVE_NFE },
                        create: {
                            chave_nfe: inv.CHAVE_NFE,
                            emitente: inv.NOME_EMITENTE || 'Desconhecido',
                            cnpj_emitente: inv.CPF_CNPJ_EMITENTE,
                            data_emissao: new Date(inv.DATA_EMISSAO),
                            valor_total: valorTotal,
                            xml_completo: xmlParaPersistirCompactado,
                            status_erp: 'PENDENTE',
                            tipo_operacao: inv.TIPO_OPERACAO,
                            tipo_operacao_desc: inv.TIPO_OPERACAO_DESC
                        },
                        update: Object.assign(Object.assign({ status_erp: 'PENDENTE' }, (normalizedXmlCompleto ? { xml_completo: this.encodeXml(normalizedXmlCompleto) } : {})), { updated_at: new Date() })
                    });
                });
            }
            const upsertBatchSize = 20;
            for (let i = 0; i < upsertTasks.length; i += upsertBatchSize) {
                const chunk = upsertTasks.slice(i, i + upsertBatchSize);
                await Promise.all(chunk.map(task => task()));
            }
            if (erpKeys.size > 0) {
                await this.prisma.nfeConciliacao.updateMany({
                    where: {
                        status_erp: 'PENDENTE',
                        data_emissao: {
                            gte: startDate,
                            lte: endDate,
                        },
                        chave_nfe: { notIn: Array.from(erpKeys) },
                    },
                    data: { status_erp: 'LANCADA' }
                });
            }
            const allLocal = await this.prisma.nfeConciliacao.findMany({
                where: {
                    data_emissao: {
                        gte: startDate,
                        lte: endDate,
                    }
                },
                orderBy: { data_emissao: 'desc' },
                take: 1200
            });
            return await Promise.all(allLocal.map(async (local) => {
                const normalizedXml = await this.normalizeBlobXml(local.xml_completo);
                return {
                    CHAVE_NFE: local.chave_nfe,
                    NOME_EMITENTE: local.emitente,
                    CPF_CNPJ_EMITENTE: local.cnpj_emitente,
                    DATA_EMISSAO: local.data_emissao,
                    VALOR_TOTAL: local.valor_total,
                    STATUS_ERP: local.status_erp,
                    TIPO_OPERACAO: local.tipo_operacao,
                    TIPO_OPERACAO_DESC: local.tipo_operacao_desc,
                    XML_COMPLETO: local.xml_completo,
                    XML_TIPO: this.detectXmlType(normalizedXml || local.xml_completo),
                    TIPO_IMPOSTO: local.tipo_imposto
                };
            }));
        }
        catch (error) {
            this.logger.error('Error in syncInvoices', error, 'Sync');
            throw error;
        }
    }
    getDateRangeOrDefault(start, end) {
        const safeEnd = end ? new Date(`${end}T23:59:59.999`) : new Date();
        const parsedEnd = Number.isNaN(safeEnd.getTime()) ? new Date() : safeEnd;
        const safeStart = start
            ? new Date(`${start}T00:00:00`)
            : new Date(parsedEnd.getTime() - (90 * 24 * 60 * 60 * 1000));
        const parsedStart = Number.isNaN(safeStart.getTime())
            ? new Date(parsedEnd.getTime() - (90 * 24 * 60 * 60 * 1000))
            : safeStart;
        return {
            startDate: parsedStart,
            endDate: parsedEnd,
        };
    }
    async syncLaunchedInvoicesFromEntradaXml() {
        return this.runLaunchedInvoicesSync();
    }
    async getInvoiceByKey(chaveNfe) {
        const key = String(chaveNfe || '').trim();
        if (!key)
            return null;
        const local = await this.prisma.nfeConciliacao.findUnique({
            where: { chave_nfe: key }
        });
        if (!local)
            return null;
        const normalizedXml = await this.normalizeBlobXml(local.xml_completo);
        return {
            EMPRESA: 1,
            CHAVE_NFE: local.chave_nfe,
            NOME_EMITENTE: local.emitente,
            CPF_CNPJ_EMITENTE: local.cnpj_emitente,
            DATA_EMISSAO: local.data_emissao,
            VALOR_TOTAL: local.valor_total,
            STATUS_ERP: local.status_erp,
            TIPO_OPERACAO: local.tipo_operacao,
            TIPO_OPERACAO_DESC: local.tipo_operacao_desc,
            XML_COMPLETO: normalizedXml || local.xml_completo,
            XML_TIPO: this.detectXmlType(normalizedXml || local.xml_completo),
            TIPO_IMPOSTO: local.tipo_imposto,
        };
    }
    detectXmlType(xml) {
        const raw = String(xml || '').trim();
        if (!raw)
            return 'SEM_XML';
        const content = raw.toLowerCase();
        const hasItems = content.includes('<det') && content.includes('<prod');
        if (hasItems)
            return 'COMPLETO';
        return 'RESUMO';
    }
    async startLaunchedInvoicesSyncJob() {
        const jobId = (0, crypto_1.randomUUID)();
        const startedAt = new Date().toISOString();
        this.launchedSyncJobs.set(jobId, {
            jobId,
            status: 'running',
            totalEncontradas: 0,
            processadas: 0,
            inseridas: 0,
            ignoradas: 0,
            progresso: 0,
            logs: [`[${startedAt}] Iniciando busca de NFs lançadas...`],
            startedAt,
        });
        this.runLaunchedInvoicesSync(jobId).catch((error) => {
            this.logger.error('Error running launched invoices sync job', error, 'Sync');
        });
        return { jobId };
    }
    getLaunchedInvoicesSyncJob(jobId) {
        var _a;
        return (_a = this.launchedSyncJobs.get(jobId)) !== null && _a !== void 0 ? _a : null;
    }
    async startXmlNormalizationJob(batchSize = 500) {
        const safeBatchSize = Number.isFinite(batchSize) ? Math.min(Math.max(Math.floor(batchSize), 100), 2000) : 500;
        const jobId = (0, crypto_1.randomUUID)();
        const startedAt = new Date().toISOString();
        this.xmlNormalizationJobs.set(jobId, {
            jobId,
            status: 'running',
            total: 0,
            processadas: 0,
            normalizadas: 0,
            ignoradas: 0,
            erros: 0,
            progresso: 0,
            logs: [`[${startedAt}] Iniciando normalização global de XMLs (batch=${safeBatchSize})...`],
            startedAt,
        });
        this.runXmlNormalization(jobId, safeBatchSize).catch((error) => {
            this.logger.error('Error running XML normalization job', error, 'NormalizeXml');
        });
        return { jobId, batchSize: safeBatchSize };
    }
    getXmlNormalizationJob(jobId) {
        var _a;
        return (_a = this.xmlNormalizationJobs.get(jobId)) !== null && _a !== void 0 ? _a : null;
    }
    appendXmlNormalizationLog(jobId, message) {
        const job = this.xmlNormalizationJobs.get(jobId);
        if (!job)
            return;
        job.logs.push(`[${new Date().toISOString()}] ${message}`);
        if (job.logs.length > 300) {
            job.logs = job.logs.slice(-300);
        }
        this.xmlNormalizationJobs.set(jobId, job);
    }
    async runXmlNormalization(jobId, batchSize) {
        try {
            const total = await this.prisma.nfeConciliacao.count();
            const initialJob = this.xmlNormalizationJobs.get(jobId);
            if (!initialJob)
                return;
            initialJob.total = total;
            this.xmlNormalizationJobs.set(jobId, initialJob);
            this.appendXmlNormalizationLog(jobId, `Total de notas para verificar: ${total}`);
            let cursor;
            let processadas = 0;
            let normalizadas = 0;
            let ignoradas = 0;
            let erros = 0;
            while (true) {
                const rows = await this.prisma.nfeConciliacao.findMany(Object.assign({ select: { chave_nfe: true, xml_completo: true }, orderBy: { chave_nfe: 'asc' }, take: batchSize }, (cursor ? { cursor: { chave_nfe: cursor }, skip: 1 } : {})));
                if (!rows.length)
                    break;
                for (const row of rows) {
                    const raw = String(row.xml_completo || '').trim();
                    try {
                        if (!raw) {
                            ignoradas++;
                            processadas++;
                            continue;
                        }
                        if (raw.startsWith('<')) {
                            const compressed = this.encodeXml(raw);
                            await this.prisma.nfeConciliacao.update({
                                where: { chave_nfe: row.chave_nfe },
                                data: { xml_completo: compressed },
                            });
                            normalizadas++;
                        }
                        else {
                            const decoded = await this.decodeXml(raw);
                            if (decoded && decoded.trim().startsWith('<')) {
                                ignoradas++;
                            }
                            else {
                                ignoradas++;
                                erros++;
                            }
                        }
                    }
                    catch (_a) {
                        erros++;
                    }
                    processadas++;
                }
                cursor = rows[rows.length - 1].chave_nfe;
                const job = this.xmlNormalizationJobs.get(jobId);
                if (!job)
                    return;
                job.processadas = processadas;
                job.normalizadas = normalizadas;
                job.ignoradas = ignoradas;
                job.erros = erros;
                job.progresso = total === 0 ? 100 : Math.round((processadas / total) * 100);
                this.xmlNormalizationJobs.set(jobId, job);
                this.appendXmlNormalizationLog(jobId, `Lote concluído. Processadas ${processadas}/${total} | normalizadas ${normalizadas} | ignoradas ${ignoradas} | erros ${erros}`);
            }
            const job = this.xmlNormalizationJobs.get(jobId);
            if (!job)
                return;
            job.status = 'completed';
            job.processadas = processadas;
            job.normalizadas = normalizadas;
            job.ignoradas = ignoradas;
            job.erros = erros;
            job.progresso = 100;
            job.completedAt = new Date().toISOString();
            this.xmlNormalizationJobs.set(jobId, job);
            this.appendXmlNormalizationLog(jobId, `Concluído. Normalizadas: ${normalizadas}. Ignoradas: ${ignoradas}. Erros: ${erros}.`);
        }
        catch (error) {
            const job = this.xmlNormalizationJobs.get(jobId);
            if (job) {
                job.status = 'failed';
                job.completedAt = new Date().toISOString();
                job.errorMessage = error instanceof Error ? error.message : String(error);
                this.xmlNormalizationJobs.set(jobId, job);
            }
            this.appendXmlNormalizationLog(jobId, `Falha na normalização: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }
    appendJobLog(jobId, message) {
        if (!jobId)
            return;
        const job = this.launchedSyncJobs.get(jobId);
        if (!job)
            return;
        job.logs.push(`[${new Date().toISOString()}] ${message}`);
        if (job.logs.length > 200) {
            job.logs = job.logs.slice(-200);
        }
        this.launchedSyncJobs.set(jobId, job);
    }
    async runLaunchedInvoicesSync(jobId) {
        try {
            this.appendJobLog(jobId, 'Consultando chaves na NF_ENTRADA_XML (empresa=1)...');
            const allEntradaKeys = await this.fetchEntradaXmlKeys();
            this.appendJobLog(jobId, `Total de chaves encontradas na NF_ENTRADA_XML: ${allEntradaKeys.length}`);
            const existingLocal = await this.prisma.nfeConciliacao.findMany({
                select: { chave_nfe: true }
            });
            const existingLocalSet = new Set(existingLocal.map(i => i.chave_nfe));
            const keysToImport = allEntradaKeys.filter(k => !existingLocalSet.has(k));
            this.appendJobLog(jobId, `Chaves novas para importar: ${keysToImport.length}`);
            let inserted = 0;
            let skipped = 0;
            if (jobId) {
                const job = this.launchedSyncJobs.get(jobId);
                if (job) {
                    job.totalEncontradas = allEntradaKeys.length;
                    job.progresso = keysToImport.length === 0 ? 100 : 0;
                    this.launchedSyncJobs.set(jobId, job);
                }
            }
            const batchSize = 100;
            let processadas = 0;
            for (let offset = 0; offset < keysToImport.length; offset += batchSize) {
                const batchKeys = keysToImport.slice(offset, offset + batchSize);
                this.appendJobLog(jobId, `Carregando lote ${Math.floor(offset / batchSize) + 1} com ${batchKeys.length} chaves...`);
                const batchInvoices = await this.fetchEntradaXmlInvoicesByKeys(batchKeys);
                for (const inv of batchInvoices) {
                    const chave = String(inv.CHAVE_NFE || '').trim();
                    if (!chave) {
                        skipped++;
                        processadas++;
                        continue;
                    }
                    const normalizedXml = await this.normalizeBlobXml(inv.XML_COMPLETO) || await this.normalizeBlobXml(inv.XML_RESUMO);
                    const parsed = this.extractInvoiceMetadataFromXml(normalizedXml, chave);
                    const parsedXmlCompactado = this.encodeXml(parsed.xmlCompleto);
                    try {
                        await this.prisma.nfeConciliacao.create({
                            data: {
                                chave_nfe: chave,
                                emitente: parsed.emitente,
                                cnpj_emitente: parsed.cnpjEmitente,
                                data_emissao: parsed.dataEmissao,
                                valor_total: parsed.valorTotal,
                                xml_completo: parsedXmlCompactado,
                                status_erp: 'LANCADA',
                                tipo_operacao: parsed.tipoOperacao,
                                tipo_operacao_desc: parsed.tipoOperacaoDesc,
                            }
                        });
                        inserted++;
                    }
                    catch (_a) {
                        skipped++;
                    }
                    processadas++;
                    if (jobId) {
                        const job = this.launchedSyncJobs.get(jobId);
                        if (job) {
                            job.processadas = processadas;
                            job.inseridas = inserted;
                            job.ignoradas = skipped;
                            job.progresso = keysToImport.length === 0
                                ? 100
                                : Math.round((processadas / keysToImport.length) * 100);
                            this.launchedSyncJobs.set(jobId, job);
                        }
                    }
                }
                if (jobId) {
                    this.appendJobLog(jobId, `Lote concluído. Processadas ${processadas}/${keysToImport.length} (inseridas: ${inserted}, ignoradas: ${skipped})`);
                }
            }
            if (jobId) {
                const job = this.launchedSyncJobs.get(jobId);
                if (job) {
                    job.status = 'completed';
                    job.processadas = keysToImport.length;
                    job.inseridas = inserted;
                    job.ignoradas = skipped;
                    job.progresso = 100;
                    job.completedAt = new Date().toISOString();
                    this.launchedSyncJobs.set(jobId, job);
                }
                this.appendJobLog(jobId, `Concluído. Inseridas: ${inserted}. Ignoradas: ${skipped}.`);
            }
            return {
                totalEncontradas: allEntradaKeys.length,
                inseridas: inserted,
                ignoradas: skipped,
            };
        }
        catch (error) {
            this.logger.error('Error syncing launched invoices from NF_ENTRADA_XML', error, 'Sync');
            if (jobId) {
                const job = this.launchedSyncJobs.get(jobId);
                if (job) {
                    job.status = 'failed';
                    job.completedAt = new Date().toISOString();
                    job.errorMessage = error instanceof Error ? error.message : String(error);
                    this.launchedSyncJobs.set(jobId, job);
                }
                this.appendJobLog(jobId, `Falha na sincronização: ${error instanceof Error ? error.message : String(error)}`);
            }
            throw error;
        }
    }
    async fetchErpInvoices(start, end) {
        const startFilter = this.toFirebirdDateOrNull(start);
        const endFilter = this.toFirebirdDateOrNull(end);
        const dateClause = startFilter && endFilter
            ? ` AND NFD.DATA_EMISSAO BETWEEN '${startFilter}' AND '${endFilter}'`
            : ` AND NFD.DATA_EMISSAO > '01.01.2025'`;
        const sql = `
      SELECT 
          NFD.EMPRESA,
          NFD.CHAVE_NFE,
          SUBSTRING(NFD.CHAVE_NFE FROM 26 FOR 9) AS NUMERO,
          NFD.CPF_CNPJ_EMITENTE,
          NFD.NOME_EMITENTE,
          NFD.RG_IE_EMITENTE,
          NFD.DATA_EMISSAO,
          NFD.TIPO_OPERACAO,
          CASE 
              WHEN NFD.TIPO_OPERACAO = 0 THEN 'ENTRADA PRÓPRIA'
              WHEN NFD.TIPO_OPERACAO = 1 THEN 'SAÍDA'
              ELSE 'OUTROS'
          END AS TIPO_OPERACAO_DESC,
          X.XML_RESUMO,
          X.XML_COMPLETO
      FROM NFE_DISTRIBUICAO NFD
      LEFT JOIN NF_ENTRADA_XML X
             ON X.EMPRESA    = NFD.EMPRESA
            AND X.CHAVE_NFE = NFD.CHAVE_NFE
      WHERE NFD.IMPORTADA    = 'N'
        AND NFD.EMPRESA      = 1
                ${dateClause}
        order by NFD.DATA_EMISSAO desc
    `;
        const firebirdSql = sql.replace(/'/g, "''");
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;
        try {
            const rows = await this.openQuery.query(tsql, {});
            return rows;
        }
        catch (e) {
            this.logger.error("Error fetching ERP invoices", e);
            return [];
        }
    }
    async fetchEntradaXmlInvoices() {
        const sql = `
      SELECT
          X.EMPRESA,
          X.CHAVE_NFE,
          X.XML_RESUMO,
          X.XML_COMPLETO
      FROM NF_ENTRADA_XML X
      WHERE X.EMPRESA = 1
      ORDER BY X.CHAVE_NFE DESC
    `;
        const firebirdSql = sql.replace(/'/g, "''");
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;
        try {
            return await this.openQuery.query(tsql, {});
        }
        catch (e) {
            this.logger.error('Error fetching NF_ENTRADA_XML invoices', e);
            return [];
        }
    }
    async fetchEntradaXmlKeys() {
        const sql = `
      SELECT
          X.CHAVE_NFE
      FROM NF_ENTRADA_XML X
      WHERE X.EMPRESA = 1
      ORDER BY X.CHAVE_NFE DESC
    `;
        const firebirdSql = sql.replace(/'/g, "''");
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;
        const rows = await this.openQuery.query(tsql, {}, { timeout: 300000, allowZeroRows: true });
        return rows
            .map(r => String(r.CHAVE_NFE || '').trim())
            .filter(Boolean);
    }
    async fetchEntradaXmlInvoicesByKeys(keys) {
        if (!keys.length)
            return [];
        const inList = keys
            .map((k) => `'${String(k).replace(/'/g, "''")}'`)
            .join(',');
        const sql = `
      SELECT
          X.EMPRESA,
          X.CHAVE_NFE,
          X.XML_RESUMO,
          X.XML_COMPLETO
      FROM NF_ENTRADA_XML X
      WHERE X.EMPRESA = 1
        AND X.CHAVE_NFE IN (${inList})
      ORDER BY X.CHAVE_NFE DESC
    `;
        const firebirdSql = sql.replace(/'/g, "''");
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;
        return await this.openQuery.query(tsql, {}, { timeout: 300000, allowZeroRows: true });
    }
    async decodeXml(content) {
        if (!content)
            return "";
        content = content.trim();
        if (content.startsWith('<'))
            return content;
        try {
            const buffer = Buffer.from(content, 'base64');
            return zlib.gunzipSync(buffer).toString('utf-8');
        }
        catch (e) {
            return content;
        }
    }
    encodeXml(xml) {
        const content = String(xml || '').trim();
        if (!content)
            return '';
        if (!content.startsWith('<'))
            return content;
        const gz = zlib.gzipSync(Buffer.from(content, 'utf-8'));
        return gz.toString('base64');
    }
    async normalizeBlobXml(content) {
        if (!content)
            return '';
        if (Buffer.isBuffer(content)) {
            const asText = content.toString('utf-8').trim();
            if (!asText)
                return '';
            return this.decodeXml(asText);
        }
        const asString = String(content).trim();
        if (!asString)
            return '';
        return this.decodeXml(asString);
    }
    toFirebirdDateOrNull(value) {
        if (!value)
            return null;
        const d = new Date(value);
        if (Number.isNaN(d.getTime()))
            return null;
        const dd = String(d.getDate()).padStart(2, '0');
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const yyyy = d.getFullYear();
        return `${dd}.${mm}.${yyyy}`;
    }
    extractInvoiceMetadataFromXml(xml, fallbackChave) {
        var _a, _b, _c, _d, _e, _f, _g, _h;
        const emitente = ((_b = (_a = xml.match(/<xNome>([\s\S]*?)<\/xNome>/)) === null || _a === void 0 ? void 0 : _a[1]) === null || _b === void 0 ? void 0 : _b.trim()) || 'Desconhecido';
        const cnpjEmitente = ((_c = xml.match(/<CNPJ>(\d+)<\/CNPJ>/)) === null || _c === void 0 ? void 0 : _c[1])
            || ((_d = xml.match(/<CPF>(\d+)<\/CPF>/)) === null || _d === void 0 ? void 0 : _d[1])
            || null;
        const dhEmi = (_e = xml.match(/<dhEmi>([^<]+)<\/dhEmi>/)) === null || _e === void 0 ? void 0 : _e[1];
        const dEmi = (_f = xml.match(/<dEmi>([^<]+)<\/dEmi>/)) === null || _f === void 0 ? void 0 : _f[1];
        const dataEmissao = new Date(dhEmi || dEmi || Date.now());
        const safeDataEmissao = Number.isNaN(dataEmissao.getTime()) ? new Date() : dataEmissao;
        const valorTotal = parseFloat(((_g = xml.match(/<vNF>([\d\.]+)<\/vNF>/)) === null || _g === void 0 ? void 0 : _g[1]) || '0') || 0;
        const tpNf = parseInt(((_h = xml.match(/<tpNF>(\d)<\/tpNF>/)) === null || _h === void 0 ? void 0 : _h[1]) || '0', 10);
        const tipoOperacao = Number.isNaN(tpNf) ? 0 : tpNf;
        const tipoOperacaoDesc = tipoOperacao === 0 ? 'ENTRADA PRÓPRIA' : 'SAÍDA';
        const finalXml = xml && xml.includes('<') ? xml : `<chave>${fallbackChave}</chave>`;
        return {
            emitente,
            cnpjEmitente,
            dataEmissao: safeDataEmissao,
            valorTotal,
            tipoOperacao,
            tipoOperacaoDesc,
            xmlCompleto: finalXml,
        };
    }
    async isInterstateInvoice(row) {
        const xml = await this.decodeXml(row.XML_COMPLETO);
        if (!xml)
            return false;
        const match = xml.match(/infNFe\s+Id="NFe(\d{44})"/);
        if (match) {
            const uf = match[1].substring(0, 2);
            return uf !== '51';
        }
        if (row.CHAVE_NFE && row.CHAVE_NFE.length === 44) {
            return row.CHAVE_NFE.substring(0, 2) !== '51';
        }
        return false;
    }
    cleanNcm(ncm) {
        return ncm ? ncm.replace(/\./g, '').trim() : '';
    }
    findMvaInRef(ncmProduto) {
        const ncmLimpo = this.cleanNcm(ncmProduto);
        let match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo);
        if (match)
            return { mva: match.MVA, item: match.Item, matchType: 'Exato' };
        if (ncmLimpo.length >= 6) {
            match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo.substring(0, 6));
            if (match)
                return { mva: match.MVA, item: match.Item, matchType: 'Raiz 6' };
        }
        if (ncmLimpo.length >= 4) {
            match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo.substring(0, 4));
            if (match)
                return { mva: match.MVA, item: match.Item, matchType: 'Raiz 4' };
        }
        return { mva: null, item: null, matchType: 'Não Encontrado' };
    }
    async calculateStForInvoice(xmlContent, icmsInternoRate = 17.0) {
        const xmlStr = await this.decodeXml(xmlContent);
        if (!xmlStr)
            return [];
        const parser = new xml2js.Parser({ explicitArray: false });
        const result = await parser.parseStringPromise(xmlStr);
        const nfe = result.nfeProc ? result.nfeProc.NFe : result.NFe;
        if (!nfe)
            return [];
        const infNfe = nfe.infNFe;
        const chave = infNfe['$']['Id'].replace('NFe', '');
        const emit = infNfe.emit;
        const ide = infNfe.ide;
        const total = infNfe.total.ICMSTot;
        const det = Array.isArray(infNfe.det) ? infNfe.det : [infNfe.det];
        try {
            const compressedXml = this.encodeXml(xmlStr);
            await this.prisma.nfeConciliacao.upsert({
                where: { chave_nfe: chave },
                create: {
                    chave_nfe: chave,
                    emitente: emit.xNome || 'Desconhecido',
                    cnpj_emitente: emit.CNPJ || emit.CPF,
                    data_emissao: new Date(ide.dhEmi || ide.dEmi),
                    valor_total: parseFloat(total.vNF || 0),
                    xml_completo: compressedXml,
                    status_erp: 'UPLOAD',
                    tipo_operacao: parseInt(ide.tpNF || 0),
                    tipo_operacao_desc: parseInt(ide.tpNF) === 0 ? 'ENTRADA' : 'SAÍDA'
                },
                update: {
                    xml_completo: compressedXml,
                    updated_at: new Date()
                }
            });
        }
        catch (e) {
            this.logger.error(`Error upserting NFe ${chave} during calculation`, e);
        }
        const results = [];
        for (const item of det) {
            const prod = item.prod;
            const imposto = item.imposto;
            const ncm = prod.NCM;
            const { mva, item: itemRef, matchType } = this.findMvaInRef(ncm);
            const vProd = parseFloat(prod.vProd || 0);
            const vFrete = parseFloat(prod.vFrete || 0);
            const vSeg = parseFloat(prod.vSeg || 0);
            const vDesc = parseFloat(prod.vDesc || 0);
            const vOutro = parseFloat(prod.vOutro || 0);
            let vIpi = 0;
            if (imposto.IPI && imposto.IPI.IPITrib) {
                vIpi = parseFloat(imposto.IPI.IPITrib.vIPI || 0);
            }
            let vIcmsProprio = 0;
            let vStDestacado = 0;
            let pMvaNota = 0;
            let pIcmsOrigem = 0;
            let cstNota = '';
            let icmsTag = '';
            const icmsKeys = Object.keys(imposto.ICMS || {});
            for (const key of icmsKeys) {
                const vals = imposto.ICMS[key];
                icmsTag = key;
                vIcmsProprio = parseFloat(vals.vICMS || 0);
                vStDestacado = parseFloat(vals.vICMSST || 0);
                pMvaNota = parseFloat(vals.pMVAST || 0);
                if (vals.pICMS)
                    pIcmsOrigem = parseFloat(vals.pICMS);
                cstNota = String(vals.CST || vals.CSOSN || '');
            }
            let taxaOrigem = 0.07;
            if (pIcmsOrigem > 0.00 && pIcmsOrigem <= 7.0) {
                taxaOrigem = pIcmsOrigem / 100.0;
            }
            const baseCreditoOrigem = vProd + vFrete + vSeg + vOutro - vDesc;
            const vCreditoOrigem = baseCreditoOrigem * taxaOrigem;
            let vStCalculado = 0;
            let diffSt = 0;
            let status = "";
            let effectiveMatchType = matchType;
            let effectiveMva = mva;
            let isDefaultMva = false;
            if (effectiveMva === null) {
                effectiveMva = 0.5039;
                isDefaultMva = true;
            }
            const baseSoma = vProd + vIpi + vFrete + vSeg + vOutro - vDesc;
            const baseCalcStRef = baseSoma * (1 + effectiveMva);
            const debitoSt = baseCalcStRef * (icmsInternoRate / 100.0);
            const vStCalculadoRaw = Math.max(0, debitoSt - vCreditoOrigem);
            vStCalculado = parseFloat(vStCalculadoRaw.toFixed(2));
            diffSt = vStCalculado - vStDestacado;
            if (!isDefaultMva) {
                if (diffSt > 0.05)
                    status = "Guia Complementar";
                else if (diffSt < -0.05)
                    status = "Pago a Maior";
                else
                    status = "OK";
            }
            else {
                if (diffSt > 0.05)
                    status = "Guia Compl. (Padrão 50%)";
                else if (diffSt < -0.05)
                    status = "Pago Maior (Padrão 50%)";
                else
                    status = "OK (Padrão 50%)";
            }
            const aliquotaInternaDecimal = icmsInternoRate / 100.0;
            const aliquotaInterestadualDIFAL = pIcmsOrigem > 0 ? pIcmsOrigem / 100.0 : 0.07;
            let vlDifalCalculado = 0;
            if (vIcmsProprio > 0) {
                const baseDifal = (baseSoma - vIcmsProprio) / (1 - aliquotaInternaDecimal);
                const difalRaw = (baseDifal * aliquotaInternaDecimal) - (baseSoma * aliquotaInterestadualDIFAL);
                vlDifalCalculado = Math.max(0, difalRaw);
            }
            else {
                const difalRaw = baseSoma * (aliquotaInternaDecimal - aliquotaInterestadualDIFAL);
                vlDifalCalculado = Math.max(0, difalRaw);
            }
            vlDifalCalculado = parseFloat(vlDifalCalculado.toFixed(2));
            results.push({
                chaveNfe: chave,
                emitente: emit.xNome,
                item: parseFloat(item['$'].nItem),
                codProd: prod.cProd,
                produto: prod.xProd,
                ncmNota: ncm,
                cfop: prod.CFOP,
                cstNota,
                icmsTag,
                possuiIcmsSt: vStDestacado > 0 || cstNota.endsWith('10') || cstNota.endsWith('60'),
                refTabela: itemRef,
                matchType: effectiveMatchType,
                mvaNota: pMvaNota,
                mvaRef: effectiveMva * 100,
                vlProduto: vProd,
                vlIcmsProprio: vIcmsProprio,
                creditoOrigem: vCreditoOrigem,
                stDestacado: vStDestacado,
                stCalculado: vStCalculado,
                vlDifal: vlDifalCalculado,
                diferenca: diffSt,
                status: status
            });
        }
        return results;
    }
    async previewFiscalConference(dto) {
        return this.runFiscalConference(dto, false);
    }
    async runFiscalConference(dto, persist) {
        const notas = Array.isArray(dto === null || dto === void 0 ? void 0 : dto.notas) ? dto.notas : [];
        const result = [];
        for (const nota of notas) {
            const chaveNfe = String((nota === null || nota === void 0 ? void 0 : nota.chaveNfe) || '').trim();
            if (!chaveNfe)
                continue;
            const nfe = await this.prisma.nfeConciliacao.findUnique({
                where: { chave_nfe: chaveNfe },
                select: { cnpj_emitente: true },
            });
            const emitenteCnpj = this.cleanDigits((nfe === null || nfe === void 0 ? void 0 : nfe.cnpj_emitente) || '');
            const isCompraDentroEstado = this.isWithinMtByChave(chaveNfe);
            const itensOut = [];
            const warnings = [];
            let hasComercializacao = false;
            let hasUsoConsumo = false;
            for (const item of Array.isArray(nota === null || nota === void 0 ? void 0 : nota.itens) ? nota.itens : []) {
                const analyzed = await this.analyzeFiscalItem({
                    chaveNfe,
                    emitenteCnpj,
                    isCompraDentroEstado,
                    item,
                });
                hasComercializacao = hasComercializacao || analyzed.destinacaoMercadoria === 'COMERCIALIZACAO';
                hasUsoConsumo = hasUsoConsumo || analyzed.destinacaoMercadoria === 'USO_CONSUMO';
                if (persist) {
                    try {
                        await this.saveFiscalConferenceItem(chaveNfe, analyzed);
                    }
                    catch (error) {
                        warnings.push(`Falha ao persistir item ${analyzed.item}: ${error instanceof Error ? error.message : String(error)}`);
                    }
                }
                itensOut.push(analyzed);
            }
            if (persist) {
                try {
                    await this.saveFiscalConferenceSummary(chaveNfe, hasComercializacao, hasUsoConsumo);
                }
                catch (error) {
                    warnings.push(`Falha ao atualizar resumo da nota: ${error instanceof Error ? error.message : String(error)}`);
                }
            }
            result.push({
                chaveNfe,
                flagsNota: {
                    compraComercializacao: hasComercializacao,
                    usoConsumo: hasUsoConsumo,
                },
                itens: itensOut,
                warnings,
            });
        }
        return { notas: result };
    }
    async analyzeFiscalItem(input) {
        const { emitenteCnpj, isCompraDentroEstado, item } = input;
        const destinacaoMercadoria = item.destinacaoMercadoria;
        const codProdFornecedorRaw = String(item.codProdFornecedor || '').trim();
        const codProdFornecedor = codProdFornecedorRaw || String(item.item || '');
        const normalizedNcm = this.cleanDigits(item.ncmNota || '');
        const normalizedCstNota = this.cleanDigits(item.cstNota || '');
        const possuiIcmsSt = Boolean(item.possuiIcmsSt || item.impostoEscolhido === 'ST');
        const possuiDifal = Boolean(item.possuiDifal || item.impostoEscolhido === 'DIFAL');
        const divergencias = [];
        const supplier = emitenteCnpj
            ? await this.findSupplierByCpfCnpj(emitenteCnpj)
            : null;
        if (!supplier) {
            divergencias.push('Fornecedor da nota não encontrado na Stage_Fornecedores pelo CPF/CNPJ do emitente.');
        }
        let vinculo = null;
        if ((supplier === null || supplier === void 0 ? void 0 : supplier.FOR_CODIGO) && codProdFornecedor) {
            vinculo = await this.findSupplierProductLink(supplier.FOR_CODIGO, codProdFornecedor);
            if (!vinculo) {
                divergencias.push('Produto do fornecedor não vinculado na Stage_Produtos_Fornecedor_NFE para o FOR_CODIGO identificado.');
            }
        }
        const produtoInterno = (vinculo === null || vinculo === void 0 ? void 0 : vinculo.PRO_CODIGO)
            ? await this.findInternalProduct(vinculo.PRO_CODIGO)
            : null;
        if ((vinculo === null || vinculo === void 0 ? void 0 : vinculo.PRO_CODIGO) && !produtoInterno) {
            divergencias.push('PRO_CODIGO vinculado não encontrado na Stage_Produtos.');
        }
        const isMonofasico = this.isMonofasicoNcm(normalizedNcm);
        const pisEsperado = isMonofasico ? '04' : 'P01';
        const cofinsEsperado = isMonofasico ? '04' : 'C01';
        if (destinacaoMercadoria === 'COMERCIALIZACAO') {
            if (isCompraDentroEstado && item.impostoEscolhido === 'ST') {
                const cstEndsWithValid = normalizedCstNota.endsWith('10') || normalizedCstNota.endsWith('60');
                if (!cstEndsWithValid) {
                    divergencias.push('Compra interna para comercialização com ST exige CST da nota final 10 ou 60.');
                }
            }
            if (produtoInterno) {
                const subtipo = String(produtoInterno.SUBTIPO || '').trim();
                if (subtipo !== '00') {
                    divergencias.push(`SUBTIPO inválido para comercialização: esperado 00 e encontrado ${subtipo || 'vazio'}.`);
                }
                if (possuiIcmsSt) {
                    const stCodigo = String(produtoInterno.ST_CODIGO || '').trim();
                    if (stCodigo !== 'ST0-X') {
                        divergencias.push(`ST_CODIGO inválido para item com ICMS ST: esperado ST0-X e encontrado ${stCodigo || 'vazio'}.`);
                    }
                }
                const pis = String(produtoInterno.PIS_CODIGO || '').trim().toUpperCase();
                const cofins = String(produtoInterno.COFINS_CODIGO || '').trim().toUpperCase();
                if (pis !== pisEsperado.toUpperCase()) {
                    divergencias.push(`PIS_CODIGO inválido: esperado ${pisEsperado} e encontrado ${pis || 'vazio'}.`);
                }
                if (cofins !== cofinsEsperado.toUpperCase()) {
                    divergencias.push(`COFINS_CODIGO inválido: esperado ${cofinsEsperado} e encontrado ${cofins || 'vazio'}.`);
                }
            }
        }
        if (destinacaoMercadoria === 'USO_CONSUMO' && produtoInterno) {
            const comercializavel = String(produtoInterno.COMERCIALIZAVEL || '').trim().toUpperCase();
            const pis = String(produtoInterno.PIS_CODIGO || '').trim().toUpperCase();
            const cofins = String(produtoInterno.COFINS_CODIGO || '').trim().toUpperCase();
            const subtipo = String(produtoInterno.SUBTIPO || '').trim();
            const subgrp = String(produtoInterno.SUBGRP_CODIGO || '').trim();
            if (comercializavel !== 'N') {
                divergencias.push(`COMERCIALIZAVEL inválido para uso e consumo: esperado N e encontrado ${comercializavel || 'vazio'}.`);
            }
            if (pis !== 'P99') {
                divergencias.push(`PIS_CODIGO inválido para uso e consumo: esperado P99 e encontrado ${pis || 'vazio'}.`);
            }
            if (cofins !== 'C99') {
                divergencias.push(`COFINS_CODIGO inválido para uso e consumo: esperado C99 e encontrado ${cofins || 'vazio'}.`);
            }
            if (subgrp !== '274') {
                divergencias.push(`SUBGRP_CODIGO inválido para uso e consumo: esperado 274 e encontrado ${subgrp || 'vazio'}.`);
            }
            if (subtipo !== '07') {
                divergencias.push(`SUBTIPO inválido para uso e consumo: esperado 07 e encontrado ${subtipo || 'vazio'}.`);
            }
        }
        return {
            item: item.item,
            codProdFornecedor,
            impostoEscolhido: item.impostoEscolhido,
            destinacaoMercadoria,
            possuiIcmsSt,
            possuiDifal,
            ncmNota: item.ncmNota || null,
            cstNota: item.cstNota || null,
            fornecedor: supplier
                ? {
                    forCodigo: String(supplier.FOR_CODIGO || ''),
                    forNome: String(supplier.FOR_NOME || ''),
                }
                : null,
            produtoVinculado: vinculo
                ? {
                    proCodigo: String(vinculo.PRO_CODIGO || ''),
                    descFornecedor: String(vinculo.DESC_PROD_FORNECEDOR || ''),
                }
                : null,
            produtoInterno: produtoInterno
                ? {
                    proCodigo: String(produtoInterno.PRO_CODIGO || ''),
                    descricao: String(produtoInterno.PRO_DESCRICAO || ''),
                    stCodigo: String(produtoInterno.ST_CODIGO || ''),
                    pisCodigo: String(produtoInterno.PIS_CODIGO || ''),
                    cofinsCodigo: String(produtoInterno.COFINS_CODIGO || ''),
                    subtipo: String(produtoInterno.SUBTIPO || ''),
                    comercializavel: String(produtoInterno.COMERCIALIZAVEL || ''),
                    subgrpCodigo: String(produtoInterno.SUBGRP_CODIGO || ''),
                }
                : null,
            monofasico: isMonofasico,
            esperadoPis: pisEsperado,
            esperadoCofins: cofinsEsperado,
            divergencias,
            statusConferencia: divergencias.length > 0 ? 'DIVERGENTE' : 'OK',
        };
    }
    async saveFiscalConferenceItem(chaveNfe, analyzed) {
        var _a, _b, _c;
        await this.prisma.$executeRawUnsafe(`
            INSERT INTO com_nfe_conciliacao_item (
                chave_nfe,
                n_item,
                cod_prod_fornecedor,
                for_codigo,
                pro_codigo,
                destinacao_mercadoria,
                imposto_escolhido,
                possui_icms_st,
                possui_difal,
                ncm_xml,
                cst_nota,
                divergencias_json,
                status_conferencia,
                created_at,
                updated_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,NOW(),NOW()
            )
            ON CONFLICT (chave_nfe, n_item)
            DO UPDATE SET
                cod_prod_fornecedor = EXCLUDED.cod_prod_fornecedor,
                for_codigo = EXCLUDED.for_codigo,
                pro_codigo = EXCLUDED.pro_codigo,
                destinacao_mercadoria = EXCLUDED.destinacao_mercadoria,
                imposto_escolhido = EXCLUDED.imposto_escolhido,
                possui_icms_st = EXCLUDED.possui_icms_st,
                possui_difal = EXCLUDED.possui_difal,
                ncm_xml = EXCLUDED.ncm_xml,
                cst_nota = EXCLUDED.cst_nota,
                divergencias_json = EXCLUDED.divergencias_json,
                status_conferencia = EXCLUDED.status_conferencia,
                updated_at = NOW()
            `, chaveNfe, analyzed.item, analyzed.codProdFornecedor, ((_a = analyzed.fornecedor) === null || _a === void 0 ? void 0 : _a.forCodigo) || null, ((_b = analyzed.produtoInterno) === null || _b === void 0 ? void 0 : _b.proCodigo) || ((_c = analyzed.produtoVinculado) === null || _c === void 0 ? void 0 : _c.proCodigo) || null, analyzed.destinacaoMercadoria, analyzed.impostoEscolhido, Boolean(analyzed.possuiIcmsSt), Boolean(analyzed.possuiDifal), analyzed.ncmNota, analyzed.cstNota, JSON.stringify(analyzed.divergencias || []), analyzed.statusConferencia);
    }
    async saveFiscalConferenceSummary(chaveNfe, compraComercializacao, usoConsumo) {
        await this.prisma.$executeRawUnsafe(`
            UPDATE com_nfe_conciliacao
            SET
                compra_comercializacao = $2,
                uso_consumo = $3,
                updated_at = NOW()
            WHERE chave_nfe = $1
            `, chaveNfe, compraComercializacao, usoConsumo);
    }
    async findSupplierByCpfCnpj(cpfCnpj) {
        var _a;
        const normalized = this.cleanDigits(cpfCnpj);
        if (!normalized)
            return null;
        const rows = await this.openQuery.query(`
            SELECT TOP 1
                FOR_CODIGO,
                FOR_NOME,
                CPF_CNPJ
            FROM [BI].[dbo].[Stage_Fornecedores]
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(CPF_CNPJ, ''), '.', ''), '/', ''), '-', ''), ' ', '') = @cpfCnpj
            ORDER BY FOR_CODIGO
            `, { cpfCnpj: normalized }, { allowZeroRows: true });
        return (_a = rows[0]) !== null && _a !== void 0 ? _a : null;
    }
    async findSupplierProductLink(forCodigo, codProdFornecedor) {
        var _a;
        const normalizedCode = String(codProdFornecedor || '').trim();
        const noLeadingZeros = normalizedCode.replace(/^0+/, '');
        const rows = await this.openQuery.query(`
            SELECT TOP 1
                EMPRESA,
                FOR_CODIGO,
                COD_PROD_FORNECEDOR,
                PRO_CODIGO,
                DESC_PROD_FORNECEDOR,
                CST_CSOSN_NOTA,
                CFOP_NOTA
            FROM [BI].[dbo].[Stage_Produtos_Fornecedor_NFE]
            WHERE FOR_CODIGO = @forCodigo
              AND (
                  LTRIM(RTRIM(ISNULL(COD_PROD_FORNECEDOR, ''))) = @codProdFornecedor
                  OR LTRIM(RTRIM(ISNULL(COD_PROD_FORNECEDOR, ''))) = @codProdFornecedorNoZero
              )
            ORDER BY EMPRESA, PRO_CODIGO
            `, {
            forCodigo,
            codProdFornecedor: normalizedCode,
            codProdFornecedorNoZero: noLeadingZeros || normalizedCode,
        }, { allowZeroRows: true });
        return (_a = rows[0]) !== null && _a !== void 0 ? _a : null;
    }
    async findInternalProduct(proCodigo) {
        var _a;
        const rows = await this.openQuery.query(`
            SELECT TOP 1
                PRO_CODIGO,
                PRO_DESCRICAO,
                ST_CODIGO,
                SUBTIPO,
                PIS_CODIGO,
                COFINS_CODIGO,
                COMERCIALIZAVEL,
                SUBGRP_CODIGO
            FROM [BI].[dbo].[Stage_Produtos]
            WHERE PRO_CODIGO = @proCodigo
            `, { proCodigo }, { allowZeroRows: true });
        return (_a = rows[0]) !== null && _a !== void 0 ? _a : null;
    }
    isMonofasicoNcm(ncm) {
        const ncmClean = this.cleanDigits(ncm);
        if (!ncmClean)
            return false;
        if (this.monofasicoNcmSet.has(ncmClean))
            return true;
        if (ncmClean.length >= 6 && this.monofasicoNcmSet.has(ncmClean.slice(0, 6)))
            return true;
        if (ncmClean.length >= 4 && this.monofasicoNcmSet.has(ncmClean.slice(0, 4)))
            return true;
        return false;
    }
    cleanDigits(value) {
        return String(value || '').replace(/\D/g, '');
    }
    isWithinMtByChave(chaveNfe) {
        const chave = String(chaveNfe || '').trim();
        return chave.slice(0, 2) === '51';
    }
    async savePaymentStatus(dto) {
        let fiscalConference = null;
        if (Array.isArray(dto.itens) && dto.itens.length > 0) {
            fiscalConference = await this.runFiscalConference({
                notas: [{ chaveNfe: dto.chaveNfe, itens: dto.itens }],
            }, true);
        }
        const result = await this.prisma.pagamentoGuia.upsert({
            where: { chave_nfe: dto.chaveNfe },
            create: {
                chave_nfe: dto.chaveNfe,
                valor: dto.valor || 0.0,
                observacoes: dto.observacoes || "",
                data_pagamento: new Date()
            },
            update: {
                valor: dto.valor || 0.0,
                observacoes: dto.observacoes || "",
                data_pagamento: new Date()
            }
        });
        if (dto.tipo_imposto !== undefined) {
            await this.prisma.nfeConciliacao.update({
                where: { chave_nfe: dto.chaveNfe },
                data: { tipo_imposto: dto.tipo_imposto }
            }).catch(e => this.logger.error("Error updating tipo_imposto in NfeConciliacao", e));
        }
        await fetch('http://log-service.acacessorios.local/log', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                usuario: dto.usuario,
                setor: 'Compras',
                tela: 'ICMS ST',
                acao: 'Create',
                descricao: `Guia de pagamento salva para NFe ${dto.chaveNfe} com valor ${dto.valor} e observações: ${dto.observacoes}`,
            }),
        });
        return Object.assign(Object.assign({}, result), { fiscalConference });
    }
    async getPaymentStatusMap() {
        const agruparTipoImposto = await this.prisma.nfeConciliacao.findMany({ select: { chave_nfe: true, tipo_imposto: true } });
        const all = await this.prisma.pagamentoGuia.findMany();
        const mapTipoImposto = {};
        for (const nfe of agruparTipoImposto) {
            if (nfe.tipo_imposto)
                mapTipoImposto[nfe.chave_nfe] = nfe.tipo_imposto;
        }
        const map = {};
        for (const item of all) {
            map[item.chave_nfe] = {
                status: item.observacoes,
                valor: item.valor,
                tipo_imposto: mapTipoImposto[item.chave_nfe]
            };
        }
        return map;
    }
    async getPaymentStatusByKey(chaveNfe) {
        var _a, _b, _c, _d;
        const key = String(chaveNfe || '').trim();
        if (!key)
            return null;
        const nfe = await this.prisma.nfeConciliacao.findUnique({
            where: { chave_nfe: key },
            select: { tipo_imposto: true }
        });
        const pagamento = await this.prisma.pagamentoGuia.findUnique({
            where: { chave_nfe: key }
        });
        if (!pagamento && !(nfe === null || nfe === void 0 ? void 0 : nfe.tipo_imposto)) {
            return null;
        }
        return {
            chaveNfe: key,
            status: (_a = pagamento === null || pagamento === void 0 ? void 0 : pagamento.observacoes) !== null && _a !== void 0 ? _a : null,
            valor: (_b = pagamento === null || pagamento === void 0 ? void 0 : pagamento.valor) !== null && _b !== void 0 ? _b : null,
            tipo_imposto: (_c = nfe === null || nfe === void 0 ? void 0 : nfe.tipo_imposto) !== null && _c !== void 0 ? _c : null,
            data_pagamento: (_d = pagamento === null || pagamento === void 0 ? void 0 : pagamento.data_pagamento) !== null && _d !== void 0 ? _d : null,
        };
    }
    async generateDanfe(xml) {
        return new Promise(async (resolve, reject) => {
            try {
                const decodedXml = await this.decodeXml(xml);
                const doc = await (0, node_pdf_nfe_1.gerarPDF)(decodedXml, { cancelada: false });
                const chunks = [];
                const stream = new stream_1.Writable({
                    write(chunk, encoding, callback) {
                        chunks.push(Buffer.from(chunk));
                        callback();
                    },
                });
                doc.pipe(stream);
                stream.on('finish', () => {
                    resolve(Buffer.concat(chunks));
                });
            }
            catch (error) {
                this.logger.error('Error generating DANFE', error);
                reject(error);
            }
        });
    }
    async generateDanfeZip(invoices) {
        return new Promise((resolve, reject) => {
            const archive = (0, archiver_1.default)('zip', {
                zlib: { level: 9 }
            });
            const chunks = [];
            const stream = new stream_1.Writable({
                write(chunk, encoding, callback) {
                    chunks.push(Buffer.from(chunk));
                    callback();
                },
            });
            archive.pipe(stream);
            stream.on('finish', () => {
                resolve(Buffer.concat(chunks));
            });
            archive.on('error', (err) => {
                reject(err);
            });
            (async () => {
                for (const inv of invoices) {
                    try {
                        const pdfBuffer = await this.generateDanfe(inv.xml);
                        archive.append(pdfBuffer, { name: `DANFE_${inv.chave}.pdf` });
                    }
                    catch (e) {
                        console.error(`Failed to generate PDF for ${inv.chave}`, e);
                    }
                }
                archive.finalize();
            })();
        });
    }
};
exports.IcmsService = IcmsService;
exports.IcmsService = IcmsService = IcmsService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [openquery_service_1.OpenQueryService,
        prisma_service_1.PrismaService])
], IcmsService);
//# sourceMappingURL=icms.service.js.map