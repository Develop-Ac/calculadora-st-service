import { Body, Controller, Get, Post, Query, StreamableFile, Res } from '@nestjs/common';
import { IcmsService } from './icms.service';
import { Response } from 'express';
import { ApiTags } from '@nestjs/swagger';

@ApiTags('icms')
@Controller('icms')
export class IcmsController {
    constructor(private readonly service: IcmsService) { }

    @Get('nfe-distribuicao')
    async getInvoices(@Query('start') start?: string, @Query('end') end?: string) {
        return this.service.syncInvoices(start, end);
    }

    @Post('calculate')
    async calculate(@Body() body: { xmls: string[] }) {
        // In a real scenario, we might pass keys and fetch XML from DB again, or pass XML content
        // For now assuming we pass XML content or keys.
        // Ideally the frontend sends keys, and backend fetches XML from DB to calculate.
        // Let's implement key-based calculation logic helper if needed, but for now generic:

        // For 'nfe-distribuicao', frontend has the XML_COMPLETO. 
        // It can send it back to calculate, OR backend can re-fetch.
        // Sending back is easier for "Upload" mode compatibility.

        const results = [];
        for (const xml of body.xmls) {
            const itemResults = await this.service.calculateStForInvoice(xml);
            results.push(...itemResults);
        }
        return results;
    }

    @Post('payment-status')
    async savePaymentStatus(@Body() body: any) {
        if (Array.isArray(body)) {
            const results = [];
            for (const item of body) {
                results.push(await this.service.savePaymentStatus(item));
            }
            return results;
        }
        return this.service.savePaymentStatus(body);
    }

    @Get('payment-status')
    async getPaymentStatus() {
        return this.service.getPaymentStatusMap();
    }
    @Post('danfe')
    async generateDanfe(@Body() body: { xml: string }, @Res({ passthrough: true }) res: Response) {
        const buffer = await this.service.generateDanfe(body.xml);
        res.set({
            'Content-Type': 'application/pdf',
            'Content-Disposition': 'inline; filename="danfe.pdf"',
        });
        return new StreamableFile(buffer);
    }

    @Post('danfe/batch')
    async generateDanfeBatch(@Body() body: { invoices: { xml: string, chave: string }[] }, @Res({ passthrough: true }) res: Response) {
        const buffer = await this.service.generateDanfeZip(body.invoices);
        res.set({
            'Content-Type': 'application/zip',
            'Content-Disposition': 'attachment; filename="danfes.zip"',
        });
        return new StreamableFile(buffer);
    }
}
