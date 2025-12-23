import { OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as sql from 'mssql';
export type MssqlQueryParams = Record<string, {
    type?: sql.ISqlTypeFactory;
    value: any;
} | number | string | null | Date | Buffer | boolean>;
export interface MssqlQueryOptions {
    timeout?: number;
    allowZeroRows?: boolean;
}
export declare class OpenQueryService implements OnModuleDestroy {
    private readonly config;
    private readonly logger;
    private pool?;
    constructor(config: ConfigService);
    private getConfig;
    getPool(): Promise<sql.ConnectionPool>;
    query<T = any>(text: string, params?: MssqlQueryParams, opts?: MssqlQueryOptions): Promise<T[]>;
    exec(text: string, params?: MssqlQueryParams, opts?: MssqlQueryOptions): Promise<{
        rowsAffected: any;
    }>;
    dispose(): Promise<void>;
    onModuleDestroy(): Promise<void>;
    private formatSqlError;
}
