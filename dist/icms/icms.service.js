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
const mva_data_1 = require("./constants/mva-data");
const node_pdf_nfe_1 = require("@alexssmusica/node-pdf-nfe");
const archiver_1 = __importDefault(require("archiver"));
const stream_1 = require("stream");
let IcmsService = IcmsService_1 = class IcmsService {
    constructor(openQuery, prisma) {
        this.openQuery = openQuery;
        this.prisma = prisma;
        this.logger = new common_1.Logger(IcmsService_1.name);
        this.refData = [];
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
        var _a;
        try {
            const erpInvoices = await this.fetchErpInvoices(start, end);
            this.logger.log(`Fetched ${erpInvoices.length} invoices from ERP`, 'Sync');
            const erpKeys = new Set();
            for (const inv of erpInvoices) {
                erpKeys.add(inv.CHAVE_NFE);
                let valorTotal = 0;
                const vNfMatch = (_a = inv.XML_COMPLETO) === null || _a === void 0 ? void 0 : _a.match(/<vNF>([\d\.]+)<\/vNF>/);
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
                        xml_completo: inv.XML_COMPLETO || '',
                        status_erp: 'PENDENTE',
                        tipo_operacao: inv.TIPO_OPERACAO,
                        tipo_operacao_desc: inv.TIPO_OPERACAO_DESC
                    },
                    update: {
                        status_erp: 'PENDENTE',
                        updated_at: new Date()
                    }
                });
            }
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
        }
        catch (error) {
            this.logger.error('Error in syncInvoices', error, 'Sync');
            throw error;
        }
    }
    async fetchErpInvoices(start, end) {
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
        const firebirdSql = sql.replace(/'/g, "''");
        const tsql = `SELECT * FROM OPENQUERY(CONSULTA, '${firebirdSql}')`;
        try {
            const rows = await this.openQuery.query(tsql, {});
            const filtered = [];
            for (const row of rows) {
                if (await this.isInterstateInvoice(row)) {
                    filtered.push(row);
                }
            }
            return filtered;
        }
        catch (e) {
            this.logger.error("Error fetching ERP invoices", e);
            return [];
        }
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
        const det = Array.isArray(infNfe.det) ? infNfe.det : [infNfe.det];
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
            const icmsKeys = Object.keys(imposto.ICMS || {});
            for (const key of icmsKeys) {
                const vals = imposto.ICMS[key];
                vIcmsProprio = parseFloat(vals.vICMS || 0);
                vStDestacado = parseFloat(vals.vICMSST || 0);
                pMvaNota = parseFloat(vals.pMVAST || 0);
                if (vals.pICMS)
                    pIcmsOrigem = parseFloat(vals.pICMS);
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
    async savePaymentStatus(dto) {
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
        return result;
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