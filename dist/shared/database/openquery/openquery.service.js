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
var OpenQueryService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.OpenQueryService = void 0;
const common_1 = require("@nestjs/common");
const config_1 = require("@nestjs/config");
const sql = __importStar(require("mssql"));
let OpenQueryService = OpenQueryService_1 = class OpenQueryService {
    constructor(config) {
        this.config = config;
        this.logger = new common_1.Logger(OpenQueryService_1.name);
    }
    getConfig() {
        const env = (key, fallback) => { var _a; return (_a = this.config.get(key)) !== null && _a !== void 0 ? _a : fallback; };
        const server = env('MSSQL_HOST', '192.168.1.146');
        const port = Number(env('MSSQL_PORT', 1433));
        const database = env('MSSQL_DB', 'BI');
        const user = env('MSSQL_USER', 'BI_AC');
        const password = env('MSSQL_PASSWORD', 'Ac@2025acesso');
        const encrypt = String(env('MSSQL_ENCRYPT', 'false')).toLowerCase() === 'true';
        const trust = String(env('MSSQL_TRUST_CERT', 'true')).toLowerCase() === 'true';
        const requestTimeout = Number(env('MSSQL_REQUEST_TIMEOUT_MS', 3600000));
        const cancelTimeout = Number(env('MSSQL_CANCEL_TIMEOUT_MS', 3600000));
        const connectTimeout = Number(env('MSSQL_CONNECT_TIMEOUT_MS', 60000));
        const poolMax = Number(env('MSSQL_POOL_MAX', 10));
        const poolMin = Number(env('MSSQL_POOL_MIN', 0));
        const poolIdle = Number(env('MSSQL_POOL_IDLE_MS', 30000));
        return {
            server,
            port,
            database,
            user,
            password,
            options: {
                encrypt,
                trustServerCertificate: trust,
                enableArithAbort: true,
                requestTimeout,
                cancelTimeout,
                connectTimeout,
            },
            pool: {
                max: poolMax,
                min: poolMin,
                idleTimeoutMillis: poolIdle,
            },
        };
    }
    async getPool() {
        var _a, _b, _c, _d;
        if (this.pool) {
            return this.pool;
        }
        const cfg = this.getConfig();
        this.logger.log(`[MSSQL] conectando em ${cfg.server}:${cfg.port} db=${cfg.database} (encrypt=${(_a = cfg.options) === null || _a === void 0 ? void 0 : _a.encrypt}, trust=${(_b = cfg.options) === null || _b === void 0 ? void 0 : _b.trustServerCertificate})`);
        const pool = new sql.ConnectionPool(cfg);
        await pool.connect();
        try {
            const req = new sql.Request(pool);
            req.timeout = 60000;
            const test = await req.query('SELECT 1 AS ok');
            this.logger.log(`[MSSQL] conectado. Teste=${(_d = (_c = test === null || test === void 0 ? void 0 : test.recordset) === null || _c === void 0 ? void 0 : _c[0]) === null || _d === void 0 ? void 0 : _d.ok}`);
        }
        catch (error) {
            this.logger.error('[MSSQL] Falha no teste de conexao: ' + ((error === null || error === void 0 ? void 0 : error.message) || error));
            throw error;
        }
        this.pool = pool;
        return pool;
    }
    async query(text, params = {}, opts = {}) {
        var _a;
        const pool = await this.getPool();
        const req = new sql.Request(pool);
        req.timeout = (_a = opts.timeout) !== null && _a !== void 0 ? _a : 60000;
        for (const [name, raw] of Object.entries(params)) {
            if (raw && typeof raw === 'object' && 'value' in raw) {
                const param = raw;
                if (param.type) {
                    req.input(name, param.type, param.value);
                }
                else {
                    req.input(name, param.value);
                }
            }
            else {
                req.input(name, raw);
            }
        }
        try {
            const { recordset } = await req.query(text);
            if (!(recordset === null || recordset === void 0 ? void 0 : recordset.length) && !opts.allowZeroRows) {
                return [];
            }
            return recordset !== null && recordset !== void 0 ? recordset : [];
        }
        catch (error) {
            this.logger.error(this.formatSqlError('query', text, params, error));
            throw error;
        }
    }
    async exec(text, params = {}, opts = {}) {
        var _a, _b;
        const pool = await this.getPool();
        const req = new sql.Request(pool);
        req.timeout = (_a = opts.timeout) !== null && _a !== void 0 ? _a : 60000;
        for (const [name, raw] of Object.entries(params)) {
            if (raw && typeof raw === 'object' && 'value' in raw) {
                const param = raw;
                if (param.type) {
                    req.input(name, param.type, param.value);
                }
                else {
                    req.input(name, param.value);
                }
            }
            else {
                req.input(name, raw);
            }
        }
        try {
            const res = await req.batch(text);
            return { rowsAffected: (_b = res.rowsAffected) !== null && _b !== void 0 ? _b : [] };
        }
        catch (error) {
            this.logger.error(this.formatSqlError('exec', text, params, error));
            throw error;
        }
    }
    async dispose() {
        if (!this.pool) {
            return;
        }
        try {
            await this.pool.close();
            this.logger.log('[MSSQL] Pool fechado.');
        }
        catch (error) {
            this.logger.error('[MSSQL] Erro ao fechar pool: ' + ((error === null || error === void 0 ? void 0 : error.message) || error));
        }
        finally {
            this.pool = undefined;
        }
    }
    async onModuleDestroy() {
        await this.dispose();
    }
    formatSqlError(kind, text, params, error) {
        const sqlPreview = text.replace(/\s+/g, ' ').trim().slice(0, 500);
        const normalized = {};
        for (const [key, value] of Object.entries(params !== null && params !== void 0 ? params : {})) {
            if (value && typeof value === 'object' && 'value' in value) {
                normalized[key] = value.value;
            }
            else {
                normalized[key] = value;
            }
        }
        return `[MSSQL ${kind.toUpperCase()}] ${(error === null || error === void 0 ? void 0 : error.message) || error}\nSQL: ${sqlPreview}\nPARAMS: ${JSON.stringify(normalized)}`;
    }
};
exports.OpenQueryService = OpenQueryService;
exports.OpenQueryService = OpenQueryService = OpenQueryService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_1.ConfigService])
], OpenQueryService);
//# sourceMappingURL=openquery.service.js.map