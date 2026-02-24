import { Injectable, Logger } from '@nestjs/common';
import { OpenQueryService } from '../shared/database/openquery/openquery.service';
import { PrismaService } from '../prisma/prisma.service';
import * as xml2js from 'xml2js';
import * as zlib from 'zlib'; // for gzip
import { CSV_DATA_CLEAN } from './constants/mva-data';
// @ts-ignore
import { gerarPDF } from '@alexssmusica/node-pdf-nfe';
import archiver from 'archiver';
import { Writable } from 'stream';

@Injectable()
export class IcmsService {
    private readonly logger = new Logger(IcmsService.name);
    private refData: any[] = [];

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
            // 1. Fetch from ERP (OpenQuery)
            const erpInvoices = await this.fetchErpInvoices(start, end);
            this.logger.log(`Fetched ${erpInvoices.length} invoices from ERP`, 'Sync');
            const erpKeys = new Set<string>();

            // 2. Upsert ERP items to Local DB
            for (const inv of erpInvoices) {
                erpKeys.add(inv.CHAVE_NFE);

                // Extract Value from XML if possible, or use 0
                let valorTotal = 0;
                // distinct namespace handling might be needed
                const vNfMatch = inv.XML_COMPLETO?.match(/<vNF>([\d\.]+)<\/vNF>/);
                if (vNfMatch) {
                    valorTotal = parseFloat(vNfMatch[1]);
                }

                // Upsert
                await this.prisma.nfeConciliacao.upsert({
                    where: { chave_nfe: inv.CHAVE_NFE },
                    create: {
                        chave_nfe: inv.CHAVE_NFE,
                        emitente: inv.NOME_EMITENTE || 'Desconhecido',
                        cnpj_emitente: inv.CPF_CNPJ_EMITENTE,
                        data_emissao: new Date(inv.DATA_EMISSAO), // Ensure date format
                        valor_total: valorTotal,
                        xml_completo: inv.XML_COMPLETO || '',
                        status_erp: 'PENDENTE',
                        tipo_operacao: inv.TIPO_OPERACAO,
                        tipo_operacao_desc: inv.TIPO_OPERACAO_DESC
                    },
                    update: {
                        // Update PENDENTE items logic? If it was LANCADA and reappeared?
                        // If it is in ERP, it's PENDENTE.
                        status_erp: 'PENDENTE',
                        updated_at: new Date()
                    }
                });
            }

            // 3. Detect Missing Items (LANCADA)
            // Find items that are PENDENTE locally but NOT in erpInvoices
            const pendingLocal = await this.prisma.nfeConciliacao.findMany({
                where: { status_erp: 'PENDENTE' },
                select: { chave_nfe: true }
            });

            for (const local of pendingLocal) {
                if (!erpKeys.has(local.chave_nfe)) {
                    await this.prisma.nfeConciliacao.update({
                        where: { chave_nfe: local.chave_nfe },
                        data: { status_erp: 'LANCADA' }
                    });
                }
            }

            // 4. Return Merged List
            const allLocal = await this.prisma.nfeConciliacao.findMany({
                orderBy: { data_emissao: 'desc' }
            });

            return allLocal.map(local => ({
                CHAVE_NFE: local.chave_nfe,
                NOME_EMITENTE: local.emitente,
                CPF_CNPJ_EMITENTE: local.cnpj_emitente,
                DATA_EMISSAO: local.data_emissao,
                VALOR_TOTAL: local.valor_total,
                STATUS_ERP: local.status_erp,
                TIPO_OPERACAO: local.tipo_operacao,
                TIPO_OPERACAO_DESC: local.tipo_operacao_desc,
                XML_COMPLETO: local.xml_completo,
                TIPO_IMPOSTO: local.tipo_imposto
            }));
        } catch (error) {
            this.logger.error('Error in syncInvoices', error, 'Sync');
            throw error;
        }
    }


    /* Renamed original fetchInvoices to fetchErpInvoices */
    async fetchErpInvoices(start?: string, end?: string) {
        // ... (Original OpenQuery Logic) ...
        const dtInicio = start ? start : new Date().toISOString().slice(0, 10);
        const dtFim = end ? end : new Date().toISOString().slice(0, 10);

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
          X.XML_COMPLETO
      FROM NFE_DISTRIBUICAO NFD
      LEFT JOIN NF_ENTRADA_XML X
             ON X.EMPRESA    = NFD.EMPRESA
            AND X.CHAVE_NFE = NFD.CHAVE_NFE
      WHERE NFD.IMPORTADA    = 'N'
        AND NFD.EMPRESA      = 1
        AND NFD.DATA_EMISSAO > '01.01.2025'
        order by NFD.DATA_EMISSAO desc
    `;

        // Escape single quotes for MSSQL string literal
        const firebirdSql = sql.replace(/'/g, "''");
        // Wrap in OPENQUERY
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;

        try {
            const rows = await this.openQuery.query<any>(tsql, {});
            const filtered = [];
            for (const row of rows) {
                if (await this.isInterstateInvoice(row)) {
                    filtered.push(row);
                }
            }
            return filtered;
        } catch (e) {
            this.logger.error("Error fetching ERP invoices", e);
            return [];
        }
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
        const det = Array.isArray(infNfe.det) ? infNfe.det : [infNfe.det];

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

            const icmsKeys = Object.keys(imposto.ICMS || {});
            for (const key of icmsKeys) {
                const vals = imposto.ICMS[key];
                vIcmsProprio = parseFloat(vals.vICMS || 0);
                vStDestacado = parseFloat(vals.vICMSST || 0);
                pMvaNota = parseFloat(vals.pMVAST || 0);
                if (vals.pICMS) pIcmsOrigem = parseFloat(vals.pICMS);
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

    async savePaymentStatus(dto: { chaveNfe: string, valor?: number, observacoes?: string, tipo_imposto?: string, usuario?: string }) {
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

        return result;
    }

    async getPaymentStatusMap() {
        const agruparTipoImposto = await this.prisma.nfeConciliacao.findMany({ select: { chave_nfe: true, tipo_imposto: true } });
        const all = await this.prisma.pagamentoGuia.findMany();

        const mapTipoImposto = {};
        for (const nfe of agruparTipoImposto) {
            if (nfe.tipo_imposto) mapTipoImposto[nfe.chave_nfe] = nfe.tipo_imposto;
        }

        const map: Record<string, { status: string, valor: number, tipo_imposto?: string }> = {};
        for (const item of all) {
            map[item.chave_nfe] = {
                status: item.observacoes,
                valor: item.valor,
                tipo_imposto: mapTipoImposto[item.chave_nfe]
            };
        }
        return map;
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
