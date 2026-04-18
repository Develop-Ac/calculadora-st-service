import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService } from '../shared/database/openquery/openquery.service';
import { PrismaService } from '../prisma/prisma.service';
import * as xml2js from 'xml2js';
import * as zlib from 'zlib'; // for gzip
import { randomUUID } from 'crypto';
import { CSV_DATA_CLEAN } from './constants/mva-data';
import { MONOFASICO_NCM_LIST } from './constants/monofasico-ncm';
import { FiscalConferenceRequestDto, FiscalConferenceItemDto } from './dto/fiscal-conference.dto';
// @ts-ignore
import { gerarPDF } from '@alexssmusica/node-pdf-nfe';
import archiver from 'archiver';
import { Writable } from 'stream';
import * as Minio from 'minio';

type GuiaPdfExtractedData = {
    numeroDocumento: string | null;
    dataVencimento: Date | null;
    valor: number | null;
    feCte: string | null;
    numeroNfExtraido: string | null;
    feCteConfere: boolean | null;
    aviso: string | null;
    textoExtraido: string;
};

@Injectable()
export class IcmsService {
    private readonly logger = new Logger(IcmsService.name);
    private readonly minioBucket = process.env.MINIO_BUCKET || 'documentos';
    private readonly minioRegion = process.env.MINIO_REGION || 'us-east-1';
    private minioClient: Minio.Client | null = null;
    private refData: any[] = [];
    private readonly monofasicoNcmSet = new Set<string>(MONOFASICO_NCM_LIST.map((ncm) => this.cleanDigits(ncm)));
    private readonly launchedSyncJobs = new Map<string, {
        jobId: string;
        status: 'running' | 'completed' | 'failed';
        totalEncontradas: number;
        processadas: number;
        inseridas: number;
        ignoradas: number;
        progresso: number;
        logs: string[];
        startedAt: string;
        completedAt?: string;
        errorMessage?: string;
    }>();
    private readonly xmlNormalizationJobs = new Map<string, {
        jobId: string;
        status: 'running' | 'completed' | 'failed';
        total: number;
        processadas: number;
        normalizadas: number;
        ignoradas: number;
        erros: number;
        progresso: number;
        logs: string[];
        startedAt: string;
        completedAt?: string;
        errorMessage?: string;
    }>();

    constructor(
        private readonly openQuery: OpenQueryService,
        private readonly prisma: PrismaService,
    ) {
        this.parseReferenceData();
    }

    // --- REFERENCE DATA PARSING ---
    private parseReferenceData() {
        // CSV parsing logic ported
        const lines = CSV_DATA_CLEAN.split('\n').filter(l => l.trim() !== '');
        const headers = lines[0].split(';'); // Assuming first line is header: Item;CEST;NCM_SH;MVA;Descricao

        for (let i = 1; i < lines.length; i++) {
            const parts = lines[i].split(';');
            if (parts.length < 4) continue;

            const row: any = {};
            // Mapping basic columns
            row['Item'] = parseFloat(parts[0]);
            row['CEST'] = parts[1];
            row['NCM_SH'] = parts[2];
            row['MVA'] = parseFloat(parts[3]);
            row['Descricao'] = parts[4];

            // Clean NCM
            row['NCM_CLEAN'] = row['NCM_SH'].replace(/\./g, '').trim();
            this.refData.push(row);
        }
        this.logger.log(`Loaded ${this.refData.length} reference MVA items.`);
    }

    // --- ETL / SYNC ---
    async syncInvoices(start?: string, end?: string) {
        try {
            const { startDate, endDate } = this.getDateRangeOrDefault(start, end);

            // 1. Fetch from ERP (OpenQuery)
            const erpInvoices = await this.fetchErpInvoices(start, end);
            this.logger.log(`Fetched ${erpInvoices.length} invoices from ERP`, 'Sync');
            const erpKeys = new Set<string>();

            // 2. Upsert ERP items to Local DB
            const upsertTasks: Array<() => Promise<void>> = [];
            for (const inv of erpInvoices) {
                erpKeys.add(inv.CHAVE_NFE);
                upsertTasks.push(async () => {
                    const normalizedXmlCompleto = await this.normalizeBlobXml(inv.XML_COMPLETO);
                    const normalizedXmlResumo = await this.normalizeBlobXml(inv.XML_RESUMO);
                    const xmlParaPersistir = normalizedXmlCompleto || normalizedXmlResumo || '';
                    const xmlParaPersistirCompactado = this.encodeXml(xmlParaPersistir);
                    const valorTotal = this.extractValorTotalFromXml(xmlParaPersistir);

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
                        update: {
                            status_erp: 'PENDENTE',
                            ...(normalizedXmlCompleto ? { xml_completo: this.encodeXml(normalizedXmlCompleto) } : {}),
                            updated_at: new Date()
                        }
                    });
                });
            }

            const upsertBatchSize = 20;
            for (let i = 0; i < upsertTasks.length; i += upsertBatchSize) {
                const chunk = upsertTasks.slice(i, i + upsertBatchSize);
                await Promise.all(chunk.map(task => task()));
            }

            // 3. Detect Missing Items (LANCADA)
            // Atualiza em massa no recorte da consulta para evitar varrer histórico inteiro
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

            // 4. Return Merged List
            const allLocal = await this.prisma.nfeConciliacao.findMany({
                where: {
                    data_emissao: {
                        gte: startDate,
                        lte: endDate,
                    }
                },
                orderBy: { data_emissao: 'desc' }
                ,
                take: 1200
            });

            return await Promise.all(allLocal.map(async (local) => {
                const normalizedXml = await this.normalizeBlobXml(local.xml_completo);
                const xmlResolved = normalizedXml || local.xml_completo;
                const valorTotal = Number(local.valor_total || 0) > 0
                    ? Number(local.valor_total || 0)
                    : this.extractValorTotalFromXml(xmlResolved);

                return {
                    CHAVE_NFE: local.chave_nfe,
                    NOME_EMITENTE: local.emitente,
                    CPF_CNPJ_EMITENTE: local.cnpj_emitente,
                    DATA_EMISSAO: local.data_emissao,
                    VALOR_TOTAL: valorTotal,
                    STATUS_ERP: local.status_erp,
                    TIPO_OPERACAO: local.tipo_operacao,
                    TIPO_OPERACAO_DESC: local.tipo_operacao_desc,
                    XML_COMPLETO: local.xml_completo,
                    XML_TIPO: this.detectXmlType(xmlResolved),
                    TIPO_IMPOSTO: local.tipo_imposto
                };
            }));
        } catch (error) {
            this.logger.error('Error in syncInvoices', error, 'Sync');
            throw error;
        }
    }

    private getDateRangeOrDefault(start?: string, end?: string) {
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

    async getInvoiceByKey(chaveNfe: string) {
        const key = String(chaveNfe || '').trim();
        if (!key) return null;

        const local = await this.prisma.nfeConciliacao.findUnique({
            where: { chave_nfe: key }
        });

        if (!local) return null;

        const normalizedXml = await this.normalizeBlobXml(local.xml_completo);
        const xmlResolved = normalizedXml || local.xml_completo;
        const valorTotal = Number(local.valor_total || 0) > 0
            ? Number(local.valor_total || 0)
            : this.extractValorTotalFromXml(xmlResolved);

        return {
            EMPRESA: 1,
            CHAVE_NFE: local.chave_nfe,
            NOME_EMITENTE: local.emitente,
            CPF_CNPJ_EMITENTE: local.cnpj_emitente,
            DATA_EMISSAO: local.data_emissao,
            VALOR_TOTAL: valorTotal,
            STATUS_ERP: local.status_erp,
            TIPO_OPERACAO: local.tipo_operacao,
            TIPO_OPERACAO_DESC: local.tipo_operacao_desc,
            XML_COMPLETO: xmlResolved,
            XML_TIPO: this.detectXmlType(xmlResolved),
            TIPO_IMPOSTO: local.tipo_imposto,
        };
    }

    private detectXmlType(xml: string | null | undefined): 'COMPLETO' | 'RESUMO' | 'SEM_XML' {
        const raw = String(xml || '').trim();
        if (!raw) return 'SEM_XML';

        const content = raw.toLowerCase();
        const hasItems = content.includes('<det') && content.includes('<prod');
        if (hasItems) return 'COMPLETO';

        return 'RESUMO';
    }

    async startLaunchedInvoicesSyncJob() {
        const jobId = randomUUID();
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

    getLaunchedInvoicesSyncJob(jobId: string) {
        return this.launchedSyncJobs.get(jobId) ?? null;
    }

    async startXmlNormalizationJob(batchSize = 500) {
        const safeBatchSize = Number.isFinite(batchSize) ? Math.min(Math.max(Math.floor(batchSize), 100), 2000) : 500;
        const jobId = randomUUID();
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

    getXmlNormalizationJob(jobId: string) {
        return this.xmlNormalizationJobs.get(jobId) ?? null;
    }

    private appendXmlNormalizationLog(jobId: string, message: string) {
        const job = this.xmlNormalizationJobs.get(jobId);
        if (!job) return;

        job.logs.push(`[${new Date().toISOString()}] ${message}`);
        if (job.logs.length > 300) {
            job.logs = job.logs.slice(-300);
        }
        this.xmlNormalizationJobs.set(jobId, job);
    }

    private async runXmlNormalization(jobId: string, batchSize: number) {
        try {
            const total = await this.prisma.nfeConciliacao.count();
            const initialJob = this.xmlNormalizationJobs.get(jobId);
            if (!initialJob) return;

            initialJob.total = total;
            this.xmlNormalizationJobs.set(jobId, initialJob);
            this.appendXmlNormalizationLog(jobId, `Total de notas para verificar: ${total}`);

            let cursor: string | undefined;
            let processadas = 0;
            let normalizadas = 0;
            let ignoradas = 0;
            let erros = 0;

            while (true) {
                const rows = await this.prisma.nfeConciliacao.findMany({
                    select: { chave_nfe: true, xml_completo: true },
                    orderBy: { chave_nfe: 'asc' },
                    take: batchSize,
                    ...(cursor ? { cursor: { chave_nfe: cursor }, skip: 1 } : {}),
                });

                if (!rows.length) break;

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
                        } else {
                            const decoded = await this.decodeXml(raw);
                            if (decoded && decoded.trim().startsWith('<')) {
                                // Já compactado/decodificável para XML: mantemos como está.
                                ignoradas++;
                            } else {
                                // Conteúdo inválido ou inesperado, não alteramos automaticamente.
                                ignoradas++;
                                erros++;
                            }
                        }
                    } catch {
                        erros++;
                    }

                    processadas++;
                }

                cursor = rows[rows.length - 1].chave_nfe;

                const job = this.xmlNormalizationJobs.get(jobId);
                if (!job) return;

                job.processadas = processadas;
                job.normalizadas = normalizadas;
                job.ignoradas = ignoradas;
                job.erros = erros;
                job.progresso = total === 0 ? 100 : Math.round((processadas / total) * 100);
                this.xmlNormalizationJobs.set(jobId, job);

                this.appendXmlNormalizationLog(
                    jobId,
                    `Lote concluído. Processadas ${processadas}/${total} | normalizadas ${normalizadas} | ignoradas ${ignoradas} | erros ${erros}`,
                );
            }

            const job = this.xmlNormalizationJobs.get(jobId);
            if (!job) return;

            job.status = 'completed';
            job.processadas = processadas;
            job.normalizadas = normalizadas;
            job.ignoradas = ignoradas;
            job.erros = erros;
            job.progresso = 100;
            job.completedAt = new Date().toISOString();
            this.xmlNormalizationJobs.set(jobId, job);

            this.appendXmlNormalizationLog(
                jobId,
                `Concluído. Normalizadas: ${normalizadas}. Ignoradas: ${ignoradas}. Erros: ${erros}.`,
            );
        } catch (error) {
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

    private appendJobLog(jobId: string | undefined, message: string) {
        if (!jobId) return;
        const job = this.launchedSyncJobs.get(jobId);
        if (!job) return;

        job.logs.push(`[${new Date().toISOString()}] ${message}`);
        if (job.logs.length > 200) {
            job.logs = job.logs.slice(-200);
        }
        this.launchedSyncJobs.set(jobId, job);
    }

    private async runLaunchedInvoicesSync(jobId?: string) {
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
                    } catch {
                        // Se outra execução inserir no meio do caminho, tratamos como ignorada
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
        } catch (error) {
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


    /* Renamed original fetchInvoices to fetchErpInvoices */
    async fetchErpInvoices(start?: string, end?: string) {
        // ... (Original OpenQuery Logic) ...
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

        // Escape single quotes for MSSQL string literal
        const firebirdSql = sql.replace(/'/g, "''");
        // Wrap in OPENQUERY
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;

        try {
            const rows = await this.openQuery.query<any>(tsql, {});
            return rows;
        } catch (e) {
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
            return await this.openQuery.query<any>(tsql, {});
        } catch (e) {
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

        const rows = await this.openQuery.query<any>(tsql, {}, { timeout: 300000, allowZeroRows: true });
        return rows
            .map(r => String(r.CHAVE_NFE || '').trim())
            .filter(Boolean);
    }

    async fetchEntradaXmlInvoicesByKeys(keys: string[]) {
        if (!keys.length) return [];

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

        return await this.openQuery.query<any>(tsql, {}, { timeout: 300000, allowZeroRows: true });
    }

    // --- XML UTILS ---
    private async decodeXml(content: string): Promise<string> {
        if (!content) return "";
        content = content.trim();
        if (content.startsWith('<')) return content;

        try {
            const buffer = Buffer.from(content, 'base64');
            return zlib.gunzipSync(buffer).toString('utf-8');
        } catch (e) {
            return content; // Fallback
        }
    }

    private encodeXml(xml: string): string {
        const content = String(xml || '').trim();
        if (!content) return '';
        if (!content.startsWith('<')) return content;

        const gz = zlib.gzipSync(Buffer.from(content, 'utf-8'));
        return gz.toString('base64');
    }

    private async normalizeBlobXml(content: any): Promise<string> {
        if (!content) return '';

        // mssql pode devolver BLOB como Buffer
        if (Buffer.isBuffer(content)) {
            const asText = content.toString('utf-8').trim();
            if (!asText) return '';
            return this.decodeXml(asText);
        }

        const asString = String(content).trim();
        if (!asString) return '';
        return this.decodeXml(asString);
    }

    private toFirebirdDateOrNull(value?: string): string | null {
        if (!value) return null;
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return null;
        const dd = String(d.getDate()).padStart(2, '0');
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const yyyy = d.getFullYear();
        return `${dd}.${mm}.${yyyy}`;
    }

    private parseDecimal(value: unknown) {
        const raw = String(value ?? '').trim();
        if (!raw) return 0;

        let normalized = raw;
        if (normalized.includes(',') && normalized.includes('.')) {
            normalized = normalized.replace(/\./g, '').replace(',', '.');
        } else if (normalized.includes(',')) {
            normalized = normalized.replace(',', '.');
        }

        const parsed = Number.parseFloat(normalized);
        return Number.isFinite(parsed) ? parsed : 0;
    }

    private parsePtBrMoney(value: string | null | undefined) {
        const raw = String(value || '').trim();
        if (!raw) return null;

        let normalized = raw;
        if (normalized.includes(',') && normalized.includes('.')) {
            normalized = normalized.replace(/\./g, '').replace(',', '.');
        } else if (normalized.includes(',')) {
            normalized = normalized.replace(',', '.');
        }

        const parsed = Number.parseFloat(normalized);
        return Number.isFinite(parsed) ? parsed : null;
    }

    private parsePtBrDate(value: string | null | undefined) {
        const raw = String(value || '').trim();
        const match = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
        if (!match) return null;

        const [, dd, mm, yyyy] = match;
        const date = new Date(`${yyyy}-${mm}-${dd}T00:00:00`);
        return Number.isNaN(date.getTime()) ? null : date;
    }

    private extractLineField(text: string, fieldNumber: string, fieldNamePattern: string) {
        const regex = new RegExp(`${fieldNumber}\\s*-\\s*${fieldNamePattern}[\\s:.-]*([^\\n\\r]+)`, 'i');
        const match = text.match(regex);
        return match?.[1]?.trim() || null;
    }

    private extractGuiaDataFromPdfText(text: string, chaveNfe: string): GuiaPdfExtractedData {
        const compactText = String(text || '').replace(/\r/g, '');

        const numeroDocumento = this.extractLineField(compactText, '23', 'INF\\.?\\s*COMPLEMENTARES');
        const dataVencimentoRaw = this.extractLineField(compactText, '22', 'DATA\\s*VENCTO\\.?');
        const valorRaw = this.extractLineField(compactText, '31', 'VALOR');
        const info32 = this.extractLineField(compactText, '32', 'INFORMA[ÇC][ÕO]ES\\s*PREVISTAS\\s*EM\\s*INSTRU[ÇC][ÕO]ES');

        const textForFeCte = info32 || compactText;
        const normalizedTextForFeCte = textForFeCte.replace(/\s+/g, ' ');
        const numeroNfChave = String(chaveNfe || '').substring(25, 34).replace(/^0+/, '');

        const normalizeDigits = (value: string | null | undefined) =>
            String(value || '').replace(/\D/g, '').replace(/^0+/, '');

        const captureContextAfterMarker = (source: string) => {
            const marker = source.match(/NFE?\s*OU\s*CTE\s*[:\-]?/i);
            if (!marker || marker.index == null) return '';

            const start = marker.index + marker[0].length;
            const tail = source.slice(start);
            const endBySenhor = tail.search(/Senhor\s+Contribuinte/i);
            const endByNaoReceber = tail.search(/N[ÃA]O\s+RECEBER/i);

            const candidates = [endBySenhor, endByNaoReceber].filter((idx) => idx >= 0);
            const end = candidates.length > 0 ? Math.min(...candidates) : Math.min(tail.length, 260);
            return tail.slice(0, end);
        };

        const selectBestToken = (tokenList: string[]) => {
            if (tokenList.length === 0) return null;

            const exactByChave = tokenList.find((token) => normalizeDigits(token) === numeroNfChave);
            if (exactByChave) return exactByChave;

            const plausible = tokenList.find((token) => {
                const len = normalizeDigits(token).length;
                return len >= 6 && len <= 10;
            });
            return plausible || tokenList[0];
        };

        const markerContext = captureContextAfterMarker(compactText) || captureContextAfterMarker(normalizedTextForFeCte);
        const markerContextTokens = (markerContext.match(/\d[\d\s.-]{4,25}/g) || [])
            .map((token) => token.replace(/\D/g, ''))
            .filter((token) => token.length > 0);

        const markerToken = selectBestToken(markerContextTokens);

        // Prefer the number that appears right after the explicit "NFE ou CTE:" marker.
        const feCteByMarker = normalizedTextForFeCte.match(/NFE?\s*OU\s*CTE\s*[:\-]?\s*(\d{1,20})\b/i)
            || textForFeCte.match(/NFE?\s*OU\s*CTE\s*[:\-]?\s*(\d{1,20})\b/i);

        // Fallbacks for layout variations.
        const feCteFallback = normalizedTextForFeCte.match(/\b(?:NFE?|CTE|FE)\s*[:\-]?\s*(\d{1,20})\b/i)
            || textForFeCte.match(/\b(?:NFE?|CTE|FE)\s*[:\-]?\s*(\d{1,20})\b/i);

        const feCteRaw = markerToken || feCteByMarker?.[1] || feCteFallback?.[1] || null;

        const dataVencimento = this.parsePtBrDate(dataVencimentoRaw);
        const valor = this.parsePtBrMoney(valorRaw);

        const numeroNfExtraido = feCteRaw ? normalizeDigits(feCteRaw) : null;

        let feCteConfere: boolean | null = null;
        let aviso: string | null = null;

        if (numeroNfExtraido) {
            feCteConfere = numeroNfExtraido === numeroNfChave;
            if (!feCteConfere) {
                aviso = `Aviso: FE/CTE (${numeroNfExtraido}) diferente do número da NF (${numeroNfChave}).`;
            }
        }

        return {
            numeroDocumento,
            dataVencimento,
            valor,
            feCte: feCteRaw,
            numeroNfExtraido,
            feCteConfere,
            aviso,
            textoExtraido: compactText,
        };
    }

    private getMinioClient() {
        if (this.minioClient) return this.minioClient;

        const rawEndpoint = String(process.env.MINIO_ENDPOINT || '').trim();
        const accessKey = process.env.MINIO_ACCESS_KEY;
        const secretKey = process.env.MINIO_SECRET_KEY;

        if (!rawEndpoint || !accessKey || !secretKey) {
            throw new Error('Configuração MinIO incompleta: MINIO_ENDPOINT, MINIO_ACCESS_KEY e MINIO_SECRET_KEY são obrigatórios.');
        }

        let endPoint = rawEndpoint;
        let port = Number(process.env.MINIO_PORT || 9000);
        let useSSL = String(process.env.MINIO_USE_SSL || 'false').toLowerCase() === 'true';

        // Accept both raw host (s3.local) and full URL (https://s3.local:9000/).
        if (rawEndpoint.includes('://')) {
            try {
                const parsed = new URL(rawEndpoint);
                endPoint = parsed.hostname;
                if (parsed.port) {
                    const parsedPort = Number(parsed.port);
                    if (Number.isFinite(parsedPort) && parsedPort > 0) {
                        port = parsedPort;
                    }
                } else if (!process.env.MINIO_PORT) {
                    port = parsed.protocol === 'https:' ? 443 : 80;
                }

                if (!process.env.MINIO_USE_SSL) {
                    useSSL = parsed.protocol === 'https:';
                }
            } catch {
                throw new Error(`MINIO_ENDPOINT inválido: ${rawEndpoint}`);
            }
        } else {
            endPoint = rawEndpoint.replace(/^https?:\/\//i, '').replace(/\/$/, '');
        }

        this.minioClient = new Minio.Client({
            endPoint,
            port,
            useSSL,
            accessKey,
            secretKey,
        });

        return this.minioClient;
    }

    private async ensureMinioBucket() {
        const client = this.getMinioClient();
        const exists = await client.bucketExists(this.minioBucket);
        if (!exists) {
            await client.makeBucket(this.minioBucket, this.minioRegion);
        }
    }

    private normalizeUploadedFileName(fileName: string | null | undefined) {
        const raw = String(fileName || '').trim();
        if (!raw) return 'guia.pdf';

        let normalized = raw;

        // Common mojibake repair (UTF-8 bytes interpreted as Latin-1), e.g. "NÂº".
        if (/[ÃÂ]/.test(normalized)) {
            try {
                const repaired = Buffer.from(normalized, 'latin1').toString('utf8');
                if (repaired && !repaired.includes('�')) {
                    normalized = repaired;
                }
            } catch {
                // Keep original when conversion fails.
            }
        }

        normalized = normalized
            .replace(/[\u0000-\u001F\u007F]/g, '')
            .replace(/[\\/]+/g, '_')
            .trim();

        return normalized || 'guia.pdf';
    }

    private async uploadGuiaPdfToMinio(chaveNfe: string, file: { buffer: Buffer; originalname: string; mimetype: string }) {
        await this.ensureMinioBucket();
        const client = this.getMinioClient();

        const normalizedOriginalName = this.normalizeUploadedFileName(file.originalname);
        const safeFileName = String(normalizedOriginalName || 'guia.pdf').replace(/[^a-zA-Z0-9_.-]/g, '_');
        const objectPath = `notas/${chaveNfe}/${Date.now()}-${safeFileName}`;

        await client.putObject(
            this.minioBucket,
            objectPath,
            file.buffer,
            file.buffer.length,
            { 'Content-Type': file.mimetype || 'application/pdf' },
        );

        return { bucket: this.minioBucket, objectPath };
    }

    private extractTagValue(xml: string, tagName: string) {
        if (!xml) return '';
        const match = xml.match(new RegExp(`<(?:\\w+:)?${tagName}>([^<]+)<\\/(?:\\w+:)?${tagName}>`, 'i'));
        return match?.[1]?.trim() || '';
    }

    private extractValorTotalFromXml(xml: string) {
        const rawVnf = this.extractTagValue(xml, 'vNF');
        return this.parseDecimal(rawVnf);
    }

    private extractInvoiceMetadataFromXml(xml: string, fallbackChave: string) {
        const emitente = xml.match(/<xNome>([\s\S]*?)<\/xNome>/)?.[1]?.trim() || 'Desconhecido';
        const cnpjEmitente = xml.match(/<CNPJ>(\d+)<\/CNPJ>/)?.[1]
            || xml.match(/<CPF>(\d+)<\/CPF>/)?.[1]
            || null;

        const dhEmi = xml.match(/<dhEmi>([^<]+)<\/dhEmi>/)?.[1];
        const dEmi = xml.match(/<dEmi>([^<]+)<\/dEmi>/)?.[1];
        const dataEmissao = new Date(dhEmi || dEmi || Date.now());
        const safeDataEmissao = Number.isNaN(dataEmissao.getTime()) ? new Date() : dataEmissao;

        const valorTotal = this.extractValorTotalFromXml(xml);

        const tpNf = parseInt(xml.match(/<tpNF>(\d)<\/tpNF>/)?.[1] || '0', 10);
        const tipoOperacao = Number.isNaN(tpNf) ? 0 : tpNf;
        const tipoOperacaoDesc = tipoOperacao === 0 ? 'ENTRADA PRÓPRIA' : 'SAÍDA';

        // Se XML vier vazio/invalido, preserva a chave como fallback de rastreabilidade
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

    private async isInterstateInvoice(row: any): Promise<boolean> {
        // 51 is MT.
        const xml = await this.decodeXml(row.XML_COMPLETO);
        if (!xml) return false;

        // Regex check for ID
        const match = xml.match(/infNFe\s+Id="NFe(\d{44})"/);
        if (match) {
            const uf = match[1].substring(0, 2);
            return uf !== '51';
        }
        // Fallback: check DB chave if available
        if (row.CHAVE_NFE && row.CHAVE_NFE.length === 44) {
            return row.CHAVE_NFE.substring(0, 2) !== '51';
        }
        return false;
    }

    // --- CALCULATION LOGIC ---

    private cleanNcm(ncm: string) {
        return ncm ? ncm.replace(/\./g, '').trim() : '';
    }

    private findMvaInRef(ncmProduto: string) {
        const ncmLimpo = this.cleanNcm(ncmProduto);

        // 1. Exact Match
        let match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo);
        if (match) return { mva: match.MVA, item: match.Item, matchType: 'Exato' };

        // 2. 6 digits
        if (ncmLimpo.length >= 6) {
            match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo.substring(0, 6));
            if (match) return { mva: match.MVA, item: match.Item, matchType: 'Raiz 6' };
        }

        // 3. 4 digits
        if (ncmLimpo.length >= 4) {
            match = this.refData.find(r => r.NCM_CLEAN === ncmLimpo.substring(0, 4));
            if (match) return { mva: match.MVA, item: match.Item, matchType: 'Raiz 4' };
        }

        return { mva: null, item: null, matchType: 'Não Encontrado' };
    }

    // Extracts items from XML and calculates ST for each
    async calculateStForInvoice(xmlContent: string, icmsInternoRate = 17.0) {
        const xmlStr = await this.decodeXml(xmlContent);
        if (!xmlStr) return [];

        const parser = new xml2js.Parser({ explicitArray: false });
        const result = await parser.parseStringPromise(xmlStr);

        const nfe = result.nfeProc ? result.nfeProc.NFe : result.NFe;
        if (!nfe) return [];

        const infNfe = nfe.infNFe;
        const chave = infNfe['$']['Id'].replace('NFe', '');
        const emit = infNfe.emit;
        const ide = infNfe.ide;
        const total = infNfe.total.ICMSTot;
        const det = Array.isArray(infNfe.det) ? infNfe.det : [infNfe.det];

        // --- UPSERT INTO NfeConciliacao ---
        // This ensures that even XML-uploaded notes exist in the DB for status tracking
        try {
            const compressedXml = this.encodeXml(xmlStr);
            await this.prisma.nfeConciliacao.upsert({
                where: { chave_nfe: chave },
                create: {
                    chave_nfe: chave,
                    emitente: emit.xNome || 'Desconhecido',
                    cnpj_emitente: emit.CNPJ || emit.CPF,
                    data_emissao: new Date(ide.dhEmi || ide.dEmi),
                    valor_total: this.parseDecimal(total.vNF || 0),
                    xml_completo: compressedXml,
                    status_erp: 'UPLOAD', // Mark as upload to distinguish
                    tipo_operacao: parseInt(ide.tpNF || 0),
                    tipo_operacao_desc: parseInt(ide.tpNF) === 0 ? 'ENTRADA' : 'SAÍDA'
                },
                update: {
                    // Update XML if it changed or to ensure it's there
                    xml_completo: compressedXml,
                    updated_at: new Date()
                }
            });
        } catch (e) {
            this.logger.error(`Error upserting NFe ${chave} during calculation`, e);
        }

        const results = [];

        for (const item of det) {
            const prod = item.prod;
            const imposto = item.imposto;

            const ncm = prod.NCM;
            const { mva, item: itemRef, matchType } = this.findMvaInRef(ncm);

            // Values
            const vProd = parseFloat(prod.vProd || 0);
            const vFrete = parseFloat(prod.vFrete || 0);
            const vSeg = parseFloat(prod.vSeg || 0);
            const vDesc = parseFloat(prod.vDesc || 0);
            const vOutro = parseFloat(prod.vOutro || 0);

            // IPI
            let vIpi = 0;
            if (imposto.IPI && imposto.IPI.IPITrib) {
                vIpi = parseFloat(imposto.IPI.IPITrib.vIPI || 0);
            }

            // ICMS Details
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
                if (vals.pICMS) pIcmsOrigem = parseFloat(vals.pICMS);
                cstNota = String(vals.CST || vals.CSOSN || '');
            }

            // Logic for Credit Origin
            // Rule:
            // 1. If pICMS > 0 and <= 7% -> Use it.
            // 2. If pICMS > 7% -> Cap at 7%.
            // 3. If pICMS is 0 or missing -> Default to 7%.

            let taxaOrigem = 0.07; // Default covering 0, missing, or > 7%

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
                // FALLBACK MVA: 50.39%
                // Used when product is NOT found in reference list
                effectiveMva = 0.5039;
                isDefaultMva = true;
                // Keep matchType as 'Não Encontrado' to trigger selection screen
            }

            const baseSoma = vProd + vIpi + vFrete + vSeg + vOutro - vDesc;
            const baseCalcStRef = baseSoma * (1 + effectiveMva);
            const debitoSt = baseCalcStRef * (icmsInternoRate / 100.0);
            const vStCalculadoRaw = Math.max(0, debitoSt - vCreditoOrigem);
            vStCalculado = parseFloat(vStCalculadoRaw.toFixed(2));

            diffSt = vStCalculado - vStDestacado;

            if (!isDefaultMva) {
                if (diffSt > 0.05) status = "Guia Complementar";
                else if (diffSt < -0.05) status = "Pago a Maior";
                else status = "OK";
            } else {
                if (diffSt > 0.05) status = "Guia Compl. (Padrão 50%)";
                else if (diffSt < -0.05) status = "Pago Maior (Padrão 50%)";
                else status = "OK (Padrão 50%)";
            }

            // ============================================
            // CALCULO DO DIFAL
            // ============================================
            // Regra independente do MVA: Calcula o DIFAL sempre baseando-se apenas na Base de Cálculo da Operação
            const aliquotaInternaDecimal = icmsInternoRate / 100.0;
            const aliquotaInterestadualDIFAL = pIcmsOrigem > 0 ? pIcmsOrigem / 100.0 : 0.07; // Usa a taxa de origem real para DIFAL ou 7% padrão
            let vlDifalCalculado = 0;

            if (vIcmsProprio > 0) {
                // Quando há destaque de ICMS na origem
                // ICMS DIFAL = [(V oper − ICMS origem) / (1 − alíquota interna)] × alíquota interna − (V oper × alíquota interestadual)
                const baseDifal = (baseSoma - vIcmsProprio) / (1 - aliquotaInternaDecimal);
                const difalRaw = (baseDifal * aliquotaInternaDecimal) - (baseSoma * aliquotaInterestadualDIFAL);
                vlDifalCalculado = Math.max(0, difalRaw);
            } else {
                // DIFAL = Base × (Alíquota interna MT − Alíquota interestadual)
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
                vlDifal: vlDifalCalculado, // NOVO CAMPO
                diferenca: diffSt,
                status: status
            });
        }
        return results;
    }
    // --- PERSISTENCE ---

    async previewFiscalConference(dto: FiscalConferenceRequestDto) {
        return this.runFiscalConference(dto, false);
    }

    private async runFiscalConference(dto: FiscalConferenceRequestDto, persist: boolean) {
        const notas = Array.isArray(dto?.notas) ? dto.notas : [];
        const result = [];

        for (const nota of notas) {
            const chaveNfe = String(nota?.chaveNfe || '').trim();
            if (!chaveNfe) continue;

            const nfe = await this.prisma.nfeConciliacao.findUnique({
                where: { chave_nfe: chaveNfe },
                select: { cnpj_emitente: true },
            });

            const emitenteCnpj = this.cleanDigits(nfe?.cnpj_emitente || '');
            const isCompraDentroEstado = this.isWithinMtByChave(chaveNfe);

            const itensOut = [];
            const warnings: string[] = [];
            let hasComercializacao = false;
            let hasUsoConsumo = false;

            for (const item of Array.isArray(nota?.itens) ? nota.itens : []) {
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
                    } catch (error) {
                        warnings.push(`Falha ao persistir item ${analyzed.item}: ${error instanceof Error ? error.message : String(error)}`);
                    }
                }

                itensOut.push(analyzed);
            }

            if (persist) {
                try {
                    await this.saveFiscalConferenceSummary(chaveNfe, hasComercializacao, hasUsoConsumo);
                } catch (error) {
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

    private async analyzeFiscalItem(input: {
        chaveNfe: string;
        emitenteCnpj: string;
        isCompraDentroEstado: boolean;
        item: FiscalConferenceItemDto;
    }) {
        const { emitenteCnpj, isCompraDentroEstado, item } = input;
        const destinacaoMercadoria = item.destinacaoMercadoria;
        const codProdFornecedorRaw = String(item.codProdFornecedor || '').trim();
        const codProdFornecedor = codProdFornecedorRaw || String(item.item || '');
        const normalizedNcm = this.cleanDigits(item.ncmNota || '');
        const normalizedCstNota = this.cleanDigits(item.cstNota || '');
        const possuiIcmsSt = Boolean(item.possuiIcmsSt || item.impostoEscolhido === 'ST');
        const possuiDifal = Boolean(item.possuiDifal || item.impostoEscolhido === 'DIFAL');

        const divergencias: string[] = [];
        const conformidades: string[] = [];

        const supplier = emitenteCnpj
            ? await this.findSupplierByCpfCnpj(emitenteCnpj)
            : null;

        if (!supplier) {
            divergencias.push('Fornecedor da nota não encontrado na Stage_Fornecedores pelo CPF/CNPJ do emitente.');
        }

        let vinculo: any = null;
        if (supplier?.FOR_CODIGO && codProdFornecedor) {
            vinculo = await this.findSupplierProductLink(supplier.FOR_CODIGO, codProdFornecedor);
            if (!vinculo) {
                divergencias.push('Produto do fornecedor não foi relacionado ao nosso código interno no Sistema Celta. Por Favor Verifique!');
            } else {
                conformidades.push('Relacionamento do produto do fornecedor com o código interno localizado no Sistema Celta.');
            }
        }

        const produtoInterno = vinculo?.PRO_CODIGO
            ? await this.findInternalProduct(vinculo.PRO_CODIGO)
            : null;

        if (vinculo?.PRO_CODIGO && !produtoInterno) {
            divergencias.push('PRO_CODIGO vinculado não encontrado na Stage_Produtos.');
        }

        if (produtoInterno && item.impostoEscolhido === 'ST') {
            const stCodigo = String(produtoInterno.ST_CODIGO || '').trim().toUpperCase();
            if (stCodigo !== 'ST0-X') {
                divergencias.push(`ST_CODIGO inválido para item com ICMS ST: esperado ST0-X e encontrado ${stCodigo || 'vazio'}.`);
            } else {
                conformidades.push('ST_CODIGO correto para item com ICMS ST: ST0-X.');
            }
        }

        if (produtoInterno && item.impostoEscolhido === 'TRIBUTADA') {
            const stCodigoTributada = String(produtoInterno.ST_CODIGO || '').trim().toUpperCase();

            if (stCodigoTributada !== 'IGI') {
                divergencias.push(`Situação tributária inválida para item Tributado: esperado ST_CODIGO=IGI e encontrado ${stCodigoTributada || 'vazio'}.`);
            } else {
                conformidades.push('Situação tributária correta para item Tributado: ST_CODIGO=IGI.');
            }
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

                const pis = String(produtoInterno.PIS_CODIGO || '').trim().toUpperCase();
                const cofins = String(produtoInterno.COFINS_CODIGO || '').trim().toUpperCase();

                if (pis !== pisEsperado.toUpperCase()) {
                    divergencias.push(`Código do Pis inválido: esperado ${pisEsperado} e encontrado ${pis || 'vazio'}.`);
                } else {
                    conformidades.push(`Código do Pis correto: ${pisEsperado}.`);
                }
                if (cofins !== cofinsEsperado.toUpperCase()) {
                    divergencias.push(`Código do Cofins inválido: esperado ${cofinsEsperado} e encontrado ${cofins || 'vazio'}.`);
                } else {
                    conformidades.push(`Código do Cofins correto: ${cofinsEsperado}.`);
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
                divergencias.push(`Código do Pis inválido para uso e consumo: esperado P99 e encontrado ${pis || 'vazio'}.`);
            } else {
                conformidades.push('Código do Pis correto para uso e consumo: P99.');
            }
            if (cofins !== 'C99') {
                divergencias.push(`Código do Cofins inválido para uso e consumo: esperado C99 e encontrado ${cofins || 'vazio'}.`);
            } else {
                conformidades.push('Código do Cofins correto para uso e consumo: C99.');
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
            codigoProduto: String(produtoInterno?.PRO_CODIGO || vinculo?.PRO_CODIGO || ''),
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
            conformidades,
            divergencias,
            statusConferencia: divergencias.length > 0 ? 'DIVERGENTE' : 'OK',
        };
    }

    private async saveFiscalConferenceItem(chaveNfe: string, analyzed: any) {
        await this.prisma.$executeRawUnsafe(
            `
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
            `,
            chaveNfe,
            analyzed.item,
            analyzed.codProdFornecedor,
            analyzed.fornecedor?.forCodigo || null,
            analyzed.produtoInterno?.proCodigo || analyzed.produtoVinculado?.proCodigo || null,
            analyzed.destinacaoMercadoria,
            analyzed.impostoEscolhido,
            Boolean(analyzed.possuiIcmsSt),
            Boolean(analyzed.possuiDifal),
            analyzed.ncmNota,
            analyzed.cstNota,
            JSON.stringify(analyzed.divergencias || []),
            analyzed.statusConferencia,
        );
    }

    private async saveFiscalConferenceSummary(chaveNfe: string, compraComercializacao: boolean, usoConsumo: boolean) {
        await this.prisma.$executeRawUnsafe(
            `
            UPDATE com_nfe_conciliacao
            SET
                compra_comercializacao = $2,
                uso_consumo = $3,
                updated_at = NOW()
            WHERE chave_nfe = $1
            `,
            chaveNfe,
            compraComercializacao,
            usoConsumo,
        );
    }

    private async findSupplierByCpfCnpj(cpfCnpj: string) {
        const normalized = this.cleanDigits(cpfCnpj);
        if (!normalized) return null;

        const rows = await this.openQuery.query<any>(
            `
            SELECT TOP 1
                FOR_CODIGO,
                FOR_NOME,
                CPF_CNPJ
            FROM [BI].[dbo].[Stage_Fornecedores]
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(ISNULL(CPF_CNPJ, ''), '.', ''), '/', ''), '-', ''), ' ', '') = @cpfCnpj
            ORDER BY FOR_CODIGO
            `,
            { cpfCnpj: normalized },
            { allowZeroRows: true },
        );

        return rows[0] ?? null;
    }

    private async findSupplierProductLink(forCodigo: string, codProdFornecedor: string) {
        const normalizedCode = String(codProdFornecedor || '').trim();
        const noLeadingZeros = normalizedCode.replace(/^0+/, '');

        const rows = await this.openQuery.query<any>(
            `
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
            `,
            {
                forCodigo,
                codProdFornecedor: normalizedCode,
                codProdFornecedorNoZero: noLeadingZeros || normalizedCode,
            },
            { allowZeroRows: true },
        );

        return rows[0] ?? null;
    }

    private async findInternalProduct(proCodigo: string) {
        const rows = await this.openQuery.query<any>(
            `
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
            `,
            { proCodigo },
            { allowZeroRows: true },
        );

        return rows[0] ?? null;
    }

    private isMonofasicoNcm(ncm: string) {
        const ncmClean = this.cleanDigits(ncm);
        if (!ncmClean) return false;

        if (this.monofasicoNcmSet.has(ncmClean)) return true;
        if (ncmClean.length >= 6 && this.monofasicoNcmSet.has(ncmClean.slice(0, 6))) return true;
        if (ncmClean.length >= 4 && this.monofasicoNcmSet.has(ncmClean.slice(0, 4))) return true;
        return false;
    }

    private cleanDigits(value: string) {
        return String(value || '').replace(/\D/g, '');
    }

    private isWithinMtByChave(chaveNfe: string) {
        const chave = String(chaveNfe || '').trim();
        return chave.slice(0, 2) === '51';
    }

    async savePaymentStatus(dto: {
        chaveNfe: string,
        valor?: number,
        observacoes?: string,
        tipo_imposto?: string,
        usuario?: string,
        itens?: FiscalConferenceItemDto[],
    }) {
        let fiscalConference: any = null;
        if (Array.isArray(dto.itens) && dto.itens.length > 0) {
            fiscalConference = await this.runFiscalConference({
                notas: [{ chaveNfe: dto.chaveNfe, itens: dto.itens }],
            }, true);

            const selectedItems = Array.from(
                new Set(
                    dto.itens
                        .map((item) => Number(item?.item))
                        .filter((item) => Number.isFinite(item) && item > 0),
                ),
            );

            if (selectedItems.length > 0) {
                await this.prisma.$executeRawUnsafe(
                    `
                    DELETE FROM com_nfe_conciliacao_item
                    WHERE chave_nfe = $1
                      AND NOT (n_item = ANY($2::int[]))
                    `,
                    dto.chaveNfe,
                    selectedItems,
                );
            }
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

        return {
            ...result,
            fiscalConference,
        };
    }

    async getPaymentStatusMap() {
        const agruparTipoImposto = await this.prisma.nfeConciliacao.findMany({ select: { chave_nfe: true, tipo_imposto: true } });
        const all = await this.prisma.pagamentoGuia.findMany();
        const guias = await this.prisma.$queryRawUnsafe<any[]>(
            `SELECT chave_nfe, bucket_name, object_path, uploaded_at FROM com_nfe_guia_pdf`
        );

        const mapTipoImposto: Record<string, string> = {};
        for (const nfe of agruparTipoImposto) {
            if (nfe.tipo_imposto) mapTipoImposto[nfe.chave_nfe] = nfe.tipo_imposto;
        }

        const map: Record<string, { status: string, valor: number, tipo_imposto?: string, guiaGerada?: boolean, guiaPath?: string }> = {};
        for (const item of all) {
            map[item.chave_nfe] = {
                status: item.observacoes,
                valor: item.valor,
                tipo_imposto: mapTipoImposto[item.chave_nfe]
            };
        }

        for (const guia of guias) {
            const chave = String(guia.chave_nfe || '').trim();
            if (!chave) continue;

            map[chave] = {
                status: map[chave]?.status || '',
                valor: map[chave]?.valor || 0,
                tipo_imposto: map[chave]?.tipo_imposto || mapTipoImposto[chave],
                guiaGerada: true,
                guiaPath: `${guia.bucket_name}/${guia.object_path}`,
            };
        }

        return map;
    }

    async getPaymentStatusByKey(chaveNfe: string) {
        const key = String(chaveNfe || '').trim();
        if (!key) return null;

        const nfe = await this.prisma.nfeConciliacao.findUnique({
            where: { chave_nfe: key },
            select: { tipo_imposto: true }
        });

        const pagamento = await this.prisma.pagamentoGuia.findUnique({
            where: { chave_nfe: key }
        });

        const guia = await this.prisma.$queryRawUnsafe<any[]>(
            `
            SELECT
                chave_nfe,
                bucket_name,
                object_path,
                original_file_name,
                numero_documento,
                data_vencimento,
                valor,
                fe_cte,
                numero_nf_extraido,
                fe_cte_confere,
                aviso,
                uploaded_at
            FROM com_nfe_guia_pdf
            WHERE chave_nfe = $1
            `,
            key,
        );

        if (!pagamento && !nfe?.tipo_imposto && guia.length === 0) {
            return null;
        }

        const guiaData = guia[0] || null;

        const itensConciliacao = await this.prisma.$queryRawUnsafe<any[]>(
            `
            SELECT
                n_item,
                cod_prod_fornecedor,
                pro_codigo,
                destinacao_mercadoria,
                imposto_escolhido,
                possui_icms_st,
                possui_difal,
                ncm_xml,
                cst_nota,
                status_conferencia,
                updated_at
            FROM com_nfe_conciliacao_item
            WHERE chave_nfe = $1
            ORDER BY n_item ASC
            `,
            key,
        );

        return {
            chaveNfe: key,
            status: pagamento?.observacoes ?? null,
            valor: pagamento?.valor ?? null,
            tipo_imposto: nfe?.tipo_imposto ?? null,
            data_pagamento: pagamento?.data_pagamento ?? null,
            itens_conciliacao: itensConciliacao.map((item) => ({
                n_item: item.n_item,
                cod_prod_fornecedor: item.cod_prod_fornecedor,
                pro_codigo: item.pro_codigo,
                destinacao_mercadoria: item.destinacao_mercadoria,
                imposto_escolhido: item.imposto_escolhido,
                possui_icms_st: item.possui_icms_st,
                possui_difal: item.possui_difal,
                ncm_xml: item.ncm_xml,
                cst_nota: item.cst_nota,
                status_conferencia: item.status_conferencia,
                updated_at: item.updated_at,
            })),
            guia_gerada: Boolean(guiaData),
            guia: guiaData
                ? {
                    bucket: guiaData.bucket_name,
                    path: guiaData.object_path,
                    original_file_name: guiaData.original_file_name,
                    numero_documento: guiaData.numero_documento,
                    data_vencimento: guiaData.data_vencimento,
                    valor: guiaData.valor,
                    fe_cte: guiaData.fe_cte,
                    numero_nf_extraido: guiaData.numero_nf_extraido,
                    fe_cte_confere: guiaData.fe_cte_confere,
                    aviso: guiaData.aviso,
                    uploaded_at: guiaData.uploaded_at,
                }
                : null,
        };
    }

    async uploadGuiaByNfe(chaveNfe: string, file: { buffer: Buffer; originalname: string; mimetype: string }) {
        const key = String(chaveNfe || '').trim();
        if (!key) {
            throw new Error('Chave NF-e inválida.');
        }

        const normalizedOriginalName = this.normalizeUploadedFileName(file.originalname);

        const nfe = await this.prisma.nfeConciliacao.findUnique({
            where: { chave_nfe: key },
            select: { chave_nfe: true },
        });

        if (!nfe) {
            throw new Error(`NF não encontrada para vínculo da guia: ${key}`);
        }

        const pdfParseModule: any = await import('pdf-parse');
        const PDFParseClass = pdfParseModule?.PDFParse;
        if (typeof PDFParseClass !== 'function') {
            throw new Error('Biblioteca de leitura de PDF incompatível: classe PDFParse não encontrada.');
        }

        const parser = new PDFParseClass({ data: file.buffer });
        let parsedText = '';
        try {
            const parsed = await parser.getText();
            parsedText = String(parsed?.text || '');
        } finally {
            await parser.destroy().catch(() => undefined);
        }

        const extracted = this.extractGuiaDataFromPdfText(parsedText, key);

        const upload = await this.uploadGuiaPdfToMinio(key, { ...file, originalname: normalizedOriginalName });

        await this.prisma.$executeRawUnsafe(
            `
            INSERT INTO com_nfe_guia_pdf (
                chave_nfe,
                bucket_name,
                object_path,
                original_file_name,
                numero_documento,
                data_vencimento,
                valor,
                fe_cte,
                numero_nf_extraido,
                fe_cte_confere,
                aviso,
                updated_at,
                uploaded_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),NOW()
            )
            ON CONFLICT (chave_nfe)
            DO UPDATE SET
                bucket_name = EXCLUDED.bucket_name,
                object_path = EXCLUDED.object_path,
                original_file_name = EXCLUDED.original_file_name,
                numero_documento = EXCLUDED.numero_documento,
                data_vencimento = EXCLUDED.data_vencimento,
                valor = EXCLUDED.valor,
                fe_cte = EXCLUDED.fe_cte,
                numero_nf_extraido = EXCLUDED.numero_nf_extraido,
                fe_cte_confere = EXCLUDED.fe_cte_confere,
                aviso = EXCLUDED.aviso,
                updated_at = NOW(),
                uploaded_at = NOW()
            `,
            key,
            upload.bucket,
            upload.objectPath,
            normalizedOriginalName,
            extracted.numeroDocumento,
            extracted.dataVencimento,
            extracted.valor,
            extracted.feCte,
            extracted.numeroNfExtraido,
            extracted.feCteConfere,
            extracted.aviso,
        );

        return {
            chaveNfe: key,
            guia_gerada: true,
            bucket: upload.bucket,
            path: upload.objectPath,
            original_file_name: normalizedOriginalName,
            numero_documento: extracted.numeroDocumento,
            data_vencimento: extracted.dataVencimento,
            valor: extracted.valor,
            fe_cte: extracted.feCte,
            numero_nf_extraido: extracted.numeroNfExtraido,
            fe_cte_confere: extracted.feCteConfere,
            aviso: extracted.aviso,
        };
    }

    async getGuiaByNfe(chaveNfe: string) {
        const key = String(chaveNfe || '').trim();
        if (!key) return null;

        const rows = await this.prisma.$queryRawUnsafe<any[]>(
            `
            SELECT
                chave_nfe,
                bucket_name,
                object_path,
                original_file_name,
                numero_documento,
                data_vencimento,
                valor,
                fe_cte,
                numero_nf_extraido,
                fe_cte_confere,
                aviso,
                uploaded_at,
                updated_at
            FROM com_nfe_guia_pdf
            WHERE chave_nfe = $1
            `,
            key,
        );

        const guia = rows[0];
        if (!guia) return null;

        return {
            chaveNfe: guia.chave_nfe,
            guia_gerada: true,
            bucket: guia.bucket_name,
            path: guia.object_path,
            original_file_name: this.normalizeUploadedFileName(guia.original_file_name),
            numero_documento: guia.numero_documento,
            data_vencimento: guia.data_vencimento,
            valor: guia.valor,
            fe_cte: guia.fe_cte,
            numero_nf_extraido: guia.numero_nf_extraido,
            fe_cte_confere: guia.fe_cte_confere,
            aviso: guia.aviso,
            uploaded_at: guia.uploaded_at,
            updated_at: guia.updated_at,
        };
    }

    async downloadGuiaByNfe(chaveNfe: string) {
        const guia = await this.getGuiaByNfe(chaveNfe);
        if (!guia?.path) return null;

        const client = this.getMinioClient();
        const stream = await client.getObject(guia.bucket || this.minioBucket, guia.path);
        const fileName = this.normalizeUploadedFileName(guia.original_file_name || `guia-${String(chaveNfe || '').trim()}.pdf`);

        return { stream, fileName };
    }

    async removeGuiaByNfe(chaveNfe: string) {
        const key = String(chaveNfe || '').trim();
        if (!key) return false;

        const guia = await this.getGuiaByNfe(key);
        if (!guia) return false;

        try {
            if (guia.path) {
                const client = this.getMinioClient();
                await client.removeObject(guia.bucket || this.minioBucket, guia.path);
            }
        } catch (error) {
            this.logger.warn(`Falha ao remover objeto da guia no MinIO para NF ${key}: ${error instanceof Error ? error.message : String(error)}`);
        }

        await this.prisma.$executeRawUnsafe(
            `DELETE FROM com_nfe_guia_pdf WHERE chave_nfe = $1`,
            key,
        );

        return true;
    }

    async generateDanfe(xml: string): Promise<Buffer> {
        return new Promise(async (resolve, reject) => {
            try {
                // Decode XML if it's zipped/base64
                const decodedXml = await this.decodeXml(xml);

                const doc = await gerarPDF(decodedXml, { cancelada: false });
                const chunks: Buffer[] = [];
                const stream = new Writable({
                    write(chunk, encoding, callback) {
                        chunks.push(Buffer.from(chunk));
                        callback();
                    },
                });

                doc.pipe(stream);

                stream.on('finish', () => {
                    resolve(Buffer.concat(chunks));
                });

                // doc.end(); // Library handles this?
            } catch (error) {
                this.logger.error('Error generating DANFE', error);
                reject(error);
            }
        });
    }

    async generateDanfeZip(invoices: { xml: string, chave: string }[]): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            const archive = archiver('zip', {
                zlib: { level: 9 }
            });

            const chunks: Buffer[] = [];
            const stream = new Writable({
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
                        // usage of generateDanfe already covers decoding now
                        const pdfBuffer = await this.generateDanfe(inv.xml);
                        archive.append(pdfBuffer, { name: `DANFE_${inv.chave}.pdf` });
                    } catch (e) {
                        console.error(`Failed to generate PDF for ${inv.chave}`, e);
                    }
                }
                archive.finalize();
            })();
        });
    }
}
