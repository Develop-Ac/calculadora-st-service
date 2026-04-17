import { OpenQueryService } from '../shared/database/openquery/openquery.service';
import { PrismaService } from '../prisma/prisma.service';
import { FiscalConferenceRequestDto, FiscalConferenceItemDto } from './dto/fiscal-conference.dto';
export declare class IcmsService {
    private readonly openQuery;
    private readonly prisma;
    private readonly logger;
    private refData;
    private readonly monofasicoNcmSet;
    private readonly launchedSyncJobs;
    private readonly xmlNormalizationJobs;
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
        XML_TIPO: "COMPLETO" | "RESUMO" | "SEM_XML";
        TIPO_IMPOSTO: string;
    }[]>;
    private getDateRangeOrDefault;
    syncLaunchedInvoicesFromEntradaXml(): Promise<{
        totalEncontradas: number;
        inseridas: number;
        ignoradas: number;
    }>;
    getInvoiceByKey(chaveNfe: string): Promise<{
        EMPRESA: number;
        CHAVE_NFE: string;
        NOME_EMITENTE: string;
        CPF_CNPJ_EMITENTE: string;
        DATA_EMISSAO: Date;
        VALOR_TOTAL: number;
        STATUS_ERP: string;
        TIPO_OPERACAO: number;
        TIPO_OPERACAO_DESC: string;
        XML_COMPLETO: string;
        XML_TIPO: "COMPLETO" | "RESUMO" | "SEM_XML";
        TIPO_IMPOSTO: string;
    }>;
    private detectXmlType;
    startLaunchedInvoicesSyncJob(): Promise<{
        jobId: `${string}-${string}-${string}-${string}-${string}`;
    }>;
    getLaunchedInvoicesSyncJob(jobId: string): {
        jobId: string;
        status: "running" | "completed" | "failed";
        totalEncontradas: number;
        processadas: number;
        inseridas: number;
        ignoradas: number;
        progresso: number;
        logs: string[];
        startedAt: string;
        completedAt?: string;
        errorMessage?: string;
    };
    startXmlNormalizationJob(batchSize?: number): Promise<{
        jobId: `${string}-${string}-${string}-${string}-${string}`;
        batchSize: number;
    }>;
    getXmlNormalizationJob(jobId: string): {
        jobId: string;
        status: "running" | "completed" | "failed";
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
    };
    private appendXmlNormalizationLog;
    private runXmlNormalization;
    private appendJobLog;
    private runLaunchedInvoicesSync;
    fetchErpInvoices(start?: string, end?: string): Promise<any[]>;
    fetchEntradaXmlInvoices(): Promise<any[]>;
    fetchEntradaXmlKeys(): Promise<string[]>;
    fetchEntradaXmlInvoicesByKeys(keys: string[]): Promise<any[]>;
    private decodeXml;
    private encodeXml;
    private normalizeBlobXml;
    private toFirebirdDateOrNull;
    private extractInvoiceMetadataFromXml;
    private isInterstateInvoice;
    private cleanNcm;
    private findMvaInRef;
    calculateStForInvoice(xmlContent: string, icmsInternoRate?: number): Promise<any[]>;
    previewFiscalConference(dto: FiscalConferenceRequestDto): Promise<{
        notas: any[];
    }>;
    private runFiscalConference;
    private analyzeFiscalItem;
    private saveFiscalConferenceItem;
    private saveFiscalConferenceSummary;
    private findSupplierByCpfCnpj;
    private findSupplierProductLink;
    private findInternalProduct;
    private isMonofasicoNcm;
    private cleanDigits;
    private isWithinMtByChave;
    savePaymentStatus(dto: {
        chaveNfe: string;
        valor?: number;
        observacoes?: string;
        tipo_imposto?: string;
        usuario?: string;
        itens?: FiscalConferenceItemDto[];
    }): Promise<{
        fiscalConference: any;
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
    getPaymentStatusByKey(chaveNfe: string): Promise<{
        chaveNfe: string;
        status: string;
        valor: number;
        tipo_imposto: string;
        data_pagamento: Date;
    }>;
    generateDanfe(xml: string): Promise<Buffer>;
    generateDanfeZip(invoices: {
        xml: string;
        chave: string;
    }[]): Promise<Buffer>;
}
