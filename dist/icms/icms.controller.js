"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IcmsController = void 0;
const common_1 = require("@nestjs/common");
const icms_service_1 = require("./icms.service");
const swagger_1 = require("@nestjs/swagger");
const platform_express_1 = require("@nestjs/platform-express");
let IcmsController = class IcmsController {
    constructor(service) {
        this.service = service;
    }
    async getInvoices(start, end) {
        return this.service.syncInvoices(start, end);
    }
    async getInvoiceByKey(chaveNfe) {
        const invoice = await this.service.getInvoiceByKey(chaveNfe);
        if (!invoice) {
            throw new common_1.NotFoundException(`NF não encontrada: ${chaveNfe}`);
        }
        return invoice;
    }
    async syncLaunchedInvoices() {
        return this.service.startLaunchedInvoicesSyncJob();
    }
    async getSyncLaunchedInvoicesStatus(jobId) {
        const status = this.service.getLaunchedInvoicesSyncJob(jobId);
        if (!status) {
            throw new common_1.NotFoundException(`Job não encontrado: ${jobId}`);
        }
        return status;
    }
    async startXmlNormalization(body) {
        var _a;
        return this.service.startXmlNormalizationJob((_a = body === null || body === void 0 ? void 0 : body.batchSize) !== null && _a !== void 0 ? _a : 500);
    }
    async getXmlNormalizationStatus(jobId) {
        const status = this.service.getXmlNormalizationJob(jobId);
        if (!status) {
            throw new common_1.NotFoundException(`Job não encontrado: ${jobId}`);
        }
        return status;
    }
    async calculate(body) {
        const results = [];
        for (const xml of body.xmls) {
            const itemResults = await this.service.calculateStForInvoice(xml);
            results.push(...itemResults);
        }
        return results;
    }
    async savePaymentStatus(body) {
        if (Array.isArray(body)) {
            const results = [];
            for (const item of body) {
                results.push(await this.service.savePaymentStatus(item));
            }
            return results;
        }
        return this.service.savePaymentStatus(body);
    }
    async previewFiscalConference(body) {
        return this.service.previewFiscalConference(body);
    }
    async getPaymentStatus() {
        return this.service.getPaymentStatusMap();
    }
    async getPaymentStatusByKey(chaveNfe) {
        const status = await this.service.getPaymentStatusByKey(chaveNfe);
        if (!status) {
            throw new common_1.NotFoundException(`Status não encontrado para a NF: ${chaveNfe}`);
        }
        return status;
    }
    async uploadGuiaByNfe(chaveNfe, file) {
        var _a;
        if (!file) {
            throw new common_1.BadRequestException('Arquivo PDF da guia não enviado.');
        }
        if (!((_a = file.mimetype) === null || _a === void 0 ? void 0 : _a.toLowerCase().includes('pdf'))) {
            throw new common_1.BadRequestException('Arquivo inválido. Envie um PDF da guia.');
        }
        return this.service.uploadGuiaByNfe(chaveNfe, file);
    }
    async getGuiaByNfe(chaveNfe) {
        const guia = await this.service.getGuiaByNfe(chaveNfe);
        if (!guia) {
            throw new common_1.NotFoundException(`Guia não encontrada para a NF: ${chaveNfe}`);
        }
        return guia;
    }
    async downloadGuiaByNfe(chaveNfe, res) {
        const payload = await this.service.downloadGuiaByNfe(chaveNfe);
        if (!payload) {
            throw new common_1.NotFoundException(`Guia não encontrada para a NF: ${chaveNfe}`);
        }
        res.set({
            'Content-Type': 'application/pdf',
            'Content-Disposition': `attachment; filename="${payload.fileName}"`,
        });
        return new common_1.StreamableFile(payload.stream);
    }
    async removeGuiaByNfe(chaveNfe) {
        const removed = await this.service.removeGuiaByNfe(chaveNfe);
        if (!removed) {
            throw new common_1.NotFoundException(`Guia não encontrada para a NF: ${chaveNfe}`);
        }
        return { success: true, chaveNfe };
    }
    async generateDanfe(body, res) {
        const buffer = await this.service.generateDanfe(body.xml);
        res.set({
            'Content-Type': 'application/pdf',
            'Content-Disposition': 'inline; filename="danfe.pdf"',
        });
        return new common_1.StreamableFile(buffer);
    }
    async generateDanfeBatch(body, res) {
        const buffer = await this.service.generateDanfeZip(body.invoices);
        res.set({
            'Content-Type': 'application/zip',
            'Content-Disposition': 'attachment; filename="danfes.zip"',
        });
        return new common_1.StreamableFile(buffer);
    }
};
exports.IcmsController = IcmsController;
__decorate([
    (0, common_1.Get)('nfe-distribuicao'),
    __param(0, (0, common_1.Query)('start')),
    __param(1, (0, common_1.Query)('end')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getInvoices", null);
__decorate([
    (0, common_1.Get)('nfe-distribuicao/:chaveNfe'),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getInvoiceByKey", null);
__decorate([
    (0, common_1.Post)('nfe-lancadas/sync'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "syncLaunchedInvoices", null);
__decorate([
    (0, common_1.Get)('nfe-lancadas/sync/:jobId'),
    __param(0, (0, common_1.Param)('jobId')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getSyncLaunchedInvoicesStatus", null);
__decorate([
    (0, common_1.Post)('xml/normalize'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "startXmlNormalization", null);
__decorate([
    (0, common_1.Get)('xml/normalize/:jobId'),
    __param(0, (0, common_1.Param)('jobId')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getXmlNormalizationStatus", null);
__decorate([
    (0, common_1.Post)('calculate'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "calculate", null);
__decorate([
    (0, common_1.Post)('payment-status'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "savePaymentStatus", null);
__decorate([
    (0, common_1.Post)('fiscal-conferencia/preview'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "previewFiscalConference", null);
__decorate([
    (0, common_1.Get)('payment-status'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getPaymentStatus", null);
__decorate([
    (0, common_1.Get)('payment-status/:chaveNfe'),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getPaymentStatusByKey", null);
__decorate([
    (0, common_1.Post)('guia/:chaveNfe/upload'),
    (0, common_1.UseInterceptors)((0, platform_express_1.FileInterceptor)('file')),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __param(1, (0, common_1.UploadedFile)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "uploadGuiaByNfe", null);
__decorate([
    (0, common_1.Get)('guia/:chaveNfe'),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getGuiaByNfe", null);
__decorate([
    (0, common_1.Get)('guia/:chaveNfe/download'),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __param(1, (0, common_1.Res)({ passthrough: true })),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "downloadGuiaByNfe", null);
__decorate([
    (0, common_1.Delete)('guia/:chaveNfe'),
    __param(0, (0, common_1.Param)('chaveNfe')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "removeGuiaByNfe", null);
__decorate([
    (0, common_1.Post)('danfe'),
    __param(0, (0, common_1.Body)()),
    __param(1, (0, common_1.Res)({ passthrough: true })),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "generateDanfe", null);
__decorate([
    (0, common_1.Post)('danfe/batch'),
    __param(0, (0, common_1.Body)()),
    __param(1, (0, common_1.Res)({ passthrough: true })),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "generateDanfeBatch", null);
exports.IcmsController = IcmsController = __decorate([
    (0, swagger_1.ApiTags)('icms'),
    (0, common_1.Controller)('icms'),
    __metadata("design:paramtypes", [icms_service_1.IcmsService])
], IcmsController);
//# sourceMappingURL=icms.controller.js.map