import { StreamableFile } from '@nestjs/common';
import { IcmsService } from './icms.service';
import { Response } from 'express';
import { FiscalConferenceRequestDto } from './dto/fiscal-conference.dto';
export declare class IcmsController {
    private readonly service;
    constructor(service: IcmsService);
    getInvoices(start?: string, end?: string): Promise<{
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
    syncLaunchedInvoices(): Promise<{
        jobId: `${string}-${string}-${string}-${string}-${string}`;
    }>;
    getSyncLaunchedInvoicesStatus(jobId: string): Promise<{
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
    }>;
    startXmlNormalization(body?: {
        batchSize?: number;
    }): Promise<{
        jobId: `${string}-${string}-${string}-${string}-${string}`;
        batchSize: number;
    }>;
    getXmlNormalizationStatus(jobId: string): Promise<{
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
    }>;
    calculate(body: {
        xmls: string[];
    }): Promise<any[]>;
    savePaymentStatus(body: any): Promise<any[] | {
        fiscalConference: any;
        chave_nfe: string;
        data_pagamento: Date;
        valor: number;
        observacoes: string;
    }>;
    previewFiscalConference(body: FiscalConferenceRequestDto): Promise<{
        notas: any[];
    }>;
    getPaymentStatus(): Promise<Record<string, {
        status: string;
        valor: number;
        tipo_imposto?: string;
        guiaGerada?: boolean;
        guiaPath?: string;
    }>>;
    getPaymentStatusByKey(chaveNfe: string): Promise<{
        chaveNfe: string;
        status: string;
        valor: number;
        tipo_imposto: string;
        data_pagamento: Date;
        guia_gerada: boolean;
        guia: {
            bucket: any;
            path: any;
            original_file_name: any;
            numero_documento: any;
            data_vencimento: any;
            valor: any;
            fe_cte: any;
            numero_nf_extraido: any;
            fe_cte_confere: any;
            aviso: any;
            uploaded_at: any;
        };
    }>;
    uploadGuiaByNfe(chaveNfe: string, file?: any): Promise<{
        chaveNfe: string;
        guia_gerada: boolean;
        bucket: string;
        path: string;
        original_file_name: string;
        numero_documento: string;
        data_vencimento: Date;
        valor: number;
        fe_cte: string;
        numero_nf_extraido: string;
        fe_cte_confere: boolean;
        aviso: string;
    }>;
    getGuiaByNfe(chaveNfe: string): Promise<{
        chaveNfe: any;
        guia_gerada: boolean;
        bucket: any;
        path: any;
        original_file_name: any;
        numero_documento: any;
        data_vencimento: any;
        valor: any;
        fe_cte: any;
        numero_nf_extraido: any;
        fe_cte_confere: any;
        aviso: any;
        uploaded_at: any;
        updated_at: any;
    }>;
    downloadGuiaByNfe(chaveNfe: string, res: Response): Promise<StreamableFile>;
    removeGuiaByNfe(chaveNfe: string): Promise<{
        success: boolean;
        chaveNfe: string;
    }>;
    generateDanfe(body: {
        xml: string;
    }, res: Response): Promise<StreamableFile>;
    generateDanfeBatch(body: {
        invoices: {
            xml: string;
            chave: string;
        }[];
    }, res: Response): Promise<StreamableFile>;
}
