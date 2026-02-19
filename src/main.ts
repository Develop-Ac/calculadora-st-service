import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import * as bodyParser from 'body-parser';
import helmet from 'helmet';
import * as dotenv from 'dotenv';
import { AppModule } from './app.module';

async function bootstrap() {
    Logger.log('Starting bootstrap...', 'Bootstrap');
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

    Logger.log(`Raw CORS_ORIGIN env: '${process.env.CORS_ORIGIN}'`, 'Bootstrap');
    const corsOrigins = process.env.CORS_ORIGIN?.split(',').map((origin) => origin.trim()) ?? true;
    Logger.log(`CORS Origins configured: ${JSON.stringify(corsOrigins)}`, 'Bootstrap');

    app.enableCors({
        origin: corsOrigins,
        credentials: true,
        methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    });

    app.setGlobalPrefix('api');

    const config = new DocumentBuilder()
        .setTitle('Calculadora ICMS ST API')
        .setDescription('API para cálculo de ICMS ST e geração de DANFE')
        .setVersion('1.0')
        .addTag('icms')
        .build();
    const document = SwaggerModule.createDocument(app, config);
    SwaggerModule.setup('api/docs', app, document);

    const httpServer = app.getHttpAdapter().getInstance();
    httpServer.get('/', (req, res) => {
        const requesterIp =
            req.headers['x-forwarded-for'] ?? req.socket?.remoteAddress ?? req.connection?.remoteAddress ?? 'unknown';
        Logger.log(`Requisicao de status recebida de ${requesterIp}`, 'Bootstrap');
        res.status(200).json({
            status: 'online',
            message: 'O servidor está online e funcional',
            docs: '/api/docs',
            timestamp: new Date().toISOString()
        });
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

    // Graceful shutdown
    process.on('SIGTERM', () => {
        Logger.log('Received SIGTERM signal. Closing http server...', 'Bootstrap');
        app.close();
    });

    await app.listen(port, '0.0.0.0');

    const url = await app.getUrl();

    Logger.log('------------------------------------------------------', 'Bootstrap');
    Logger.log(`🚀  Service ready and listening!`, 'Bootstrap');
    Logger.log(`------------------------------------------------------`, 'Bootstrap');
    Logger.log(`🟢  Local:   ${url}`, 'Bootstrap');
    Logger.log(`🟢  Network: http://0.0.0.0:${port}`, 'Bootstrap');
    Logger.log(`📄  Swagger: ${url}/api/docs`, 'Bootstrap');
    Logger.log(`------------------------------------------------------`, 'Bootstrap');
}

bootstrap().catch(err => {
    Logger.error('Fatal error during application bootstrap', err, 'Bootstrap');
    process.exit(1);
});
