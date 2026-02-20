import { StreamableFile } from '@nestjs/common';
import { IcmsService } from './icms.service';
import { Response } from 'express';
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
        TIPO_IMPOSTO: string;
    }[]>;
    calculate(body: {
        xmls: string[];
    }): Promise<any[]>;
    savePaymentStatus(body: any): Promise<any[] | {
        chave_nfe: string;
        data_pagamento: Date;
        valor: number;
        observacoes: string;
    }>;
    getPaymentStatus(): Promise<Record<string, {
        status: string;
        valor: number;
        tipo_imposto?: string;
    }>>;
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
