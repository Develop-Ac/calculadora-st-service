import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(PrismaService.name);

    async onModuleInit() {
        this.logger.log('Connecting to database...', 'PrismaService');
        try {
            await this.$connect();
            this.logger.log('Database connected successfully.', 'PrismaService');
        } catch (e) {
            this.logger.error('Failed to connect to database', e, 'PrismaService');
            throw e;
        }
    }

    async onModuleDestroy() {
        await this.$disconnect();
    }
}
