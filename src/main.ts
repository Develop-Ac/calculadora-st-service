import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import * as bodyParser from 'body-parser';
import helmet from 'helmet';
import * as dotenv from 'dotenv';
import { AppModule } from './app.module';

async function bootstrap() {
    try { dotenv.config(); } catch (_) { }
    const app = await NestFactory.create<NestExpressApplication>(AppModule, { bufferLogs: true });

    app.use(
        helmet({
            contentSecurityPolicy: false,
            crossOriginEmbedderPolicy: false,
        }),
    );

    app.use(bodyParser.json({ limit: '50mb' }));
    app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

    const corsOrigins = process.env.CORS_ORIGIN?.split(',').map((origin) => origin.trim()) ?? true;
    Logger.log(`CORS Origins configured: ${JSON.stringify(corsOrigins)}`, 'Bootstrap');

    app.enableCors({
        origin: corsOrigins,
        credentials: true,
        methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    });

    app.setGlobalPrefix('api');

    const httpServer = app.getHttpAdapter().getInstance();
    httpServer.get('/', (req, res) => {
        const requesterIp =
            req.headers['x-forwarded-for'] ?? req.socket?.remoteAddress ?? req.connection?.remoteAddress ?? 'unknown';
        Logger.log(`Requisicao de status recebida de ${requesterIp}`, 'Bootstrap');
        res.status(200).send('API Calculadora ST ativa. Utilize o prefixo /api para acessar as rotas.');
    });

    app.useGlobalPipes(
        new ValidationPipe({
            whitelist: true,
            transform: true,
            forbidNonWhitelisted: true,
            transformOptions: { enableImplicitConversion: true },
        }),
    );

    const port = parseInt(process.env.PORT ?? '3000', 10);
    await app.listen(port, '0.0.0.0');
    Logger.log(`API Calculadora ST ativa em http://localhost:${port}`, 'Bootstrap');
}

bootstrap();
