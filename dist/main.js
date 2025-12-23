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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@nestjs/common");
const core_1 = require("@nestjs/core");
const bodyParser = __importStar(require("body-parser"));
const helmet_1 = __importDefault(require("helmet"));
const dotenv = __importStar(require("dotenv"));
const app_module_1 = require("./app.module");
async function bootstrap() {
    var _a, _b, _c;
    try {
        dotenv.config();
    }
    catch (_) { }
    const app = await core_1.NestFactory.create(app_module_1.AppModule, { bufferLogs: true });
    app.use((0, helmet_1.default)({
        contentSecurityPolicy: false,
        crossOriginEmbedderPolicy: false,
    }));
    app.use(bodyParser.json({ limit: '50mb' }));
    app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));
    const corsOrigins = (_b = (_a = process.env.CORS_ORIGIN) === null || _a === void 0 ? void 0 : _a.split(',').map((origin) => origin.trim())) !== null && _b !== void 0 ? _b : true;
    common_1.Logger.log(`CORS Origins configured: ${JSON.stringify(corsOrigins)}`, 'Bootstrap');
    app.enableCors({
        origin: corsOrigins,
        credentials: true,
        methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    });
    app.setGlobalPrefix('api');
    const httpServer = app.getHttpAdapter().getInstance();
    httpServer.get('/', (req, res) => {
        var _a, _b, _c, _d, _e;
        const requesterIp = (_e = (_c = (_a = req.headers['x-forwarded-for']) !== null && _a !== void 0 ? _a : (_b = req.socket) === null || _b === void 0 ? void 0 : _b.remoteAddress) !== null && _c !== void 0 ? _c : (_d = req.connection) === null || _d === void 0 ? void 0 : _d.remoteAddress) !== null && _e !== void 0 ? _e : 'unknown';
        common_1.Logger.log(`Requisicao de status recebida de ${requesterIp}`, 'Bootstrap');
        res.status(200).send('API Calculadora ST ativa. Utilize o prefixo /api para acessar as rotas.');
    });
    app.useGlobalPipes(new common_1.ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
        transformOptions: { enableImplicitConversion: true },
    }));
    const port = parseInt((_c = process.env.PORT) !== null && _c !== void 0 ? _c : '3000', 10);
    await app.listen(port, '0.0.0.0');
    common_1.Logger.log(`API Calculadora ST ativa em http://localhost:${port}`, 'Bootstrap');
}
bootstrap();
//# sourceMappingURL=main.js.map