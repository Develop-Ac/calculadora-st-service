import { OpenQueryService } from '../shared/database/openquery/openquery.service';
import { PrismaService } from '../prisma/prisma.service';
export declare class IcmsService {
    private readonly openQuery;
    private readonly prisma;
    private readonly logger;
    private refData;
    constructor(openQuery: OpenQueryService, prisma: PrismaService);
    private parseReferenceData;
    syncInvoices(start?: string, end?: string): Promise<{
        CHAVE_NFE: string;
        NOME_EMITENTE: string;
        CPF_CNPJ_EMITENTE: string;
        DATA_EMISSAO: Date;
        VALOR_TOTAL: number;
        STATUS_ERP: string;
        TIPO_OPERACAO: number;
        TIPO_OPERACAO_DESC: string;
        XML_COMPLETO: string;
        TIPO_IMPOSTO: string;
    }[]>;
    fetchErpInvoices(start?: string, end?: string): Promise<any[]>;
    private decodeXml;
    private isInterstateInvoice;
    private cleanNcm;
    private findMvaInRef;
    calculateStForInvoice(xmlContent: string, icmsInternoRate?: number): Promise<any[]>;
    savePaymentStatus(dto: {
        chaveNfe: string;
        valor?: number;
        observacoes?: string;
        tipo_imposto?: string;
    }): Promise<{
        chave_nfe: string;
        data_pagamento: Date;
        valor: number;
        observacoes: string;
    }>;
    getPaymentStatusMap(): Promise<Record<string, {
        status: string;
        valor: number;
        tipo_imposto?: string;
    }>>;
    generateDanfe(xml: string): Promise<Buffer>;
    generateDanfeZip(invoices: {
        xml: string;
        chave: string;
    }[]): Promise<Buffer>;
}
