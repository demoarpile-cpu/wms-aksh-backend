const cron = require('node-cron');
const reportService = require('./reportService');
const { Report } = require('../models');
const { Op } = require('sequelize');
const dayjs = require('dayjs');

function init() {
    console.log('[CRON] Initializing Report Scheduler...');

    // Run every day at 00:01 (one minute past midnight)
    cron.schedule('1 0 * * *', async () => {
        await processScheduledReports();
    });

    // Also run every hour to catch any missed or recently added schedules
    cron.schedule('0 * * * *', async () => {
        console.log('[CRON] Checking for scheduled reports...');
        await processScheduledReports();
    });
}

async function processScheduledReports() {
    try {
        // Check if table exists (to avoid crash on very first run if sync is slow)
        try {
            if (typeof Report.count === 'function') {
                await Report.count();
            } else {
                return;
            }
        } catch (e) {
            console.log('[CRON] Reports table not ready yet, skipping...');
            return;
        }

        const activeReports = await Report.findAll({
            where: {
                schedule: { [Op.ne]: 'ONCE' },
                status: 'COMPLETED'
            }
        });

        for (const report of activeReports) {
            const lastRun = dayjs(report.lastRunAt);
            const now = dayjs();
            let shouldRun = false;

            if (report.schedule === 'DAILY') {
                if (now.diff(lastRun, 'day') >= 1) shouldRun = true;
            } else if (report.schedule === 'WEEKLY') {
                if (now.diff(lastRun, 'week') >= 1) shouldRun = true;
            } else if (report.schedule === 'MONTHLY') {
                if (now.diff(lastRun, 'month') >= 1) shouldRun = true;
            }

            if (shouldRun) {
                console.log(`[CRON] Generating scheduled report: ${report.reportName} (${report.schedule})`);

                // Use a system user or super_admin context for creation
                const systemUser = {
                    id: 0,
                    role: 'super_admin',
                    companyId: report.companyId
                };

                try {
                    await reportService.create({
                        reportType: report.reportType,
                        reportName: `${report.reportName} (${now.format('YYYY-MM-DD')})`,
                        format: report.format,
                        schedule: 'ONCE', // The generated instance is ONCE, the original persists
                        companyId: report.companyId,
                        startDate: report.startDate, // Optional: adjust based on schedule?
                        endDate: report.endDate
                    }, systemUser);

                    // Update the original report template's lastRunAt
                    await report.update({ lastRunAt: new Date() });
                    console.log(`[CRON] Successfully generated: ${report.reportName}`);
                } catch (err) {
                    console.error(`[CRON] Failed to generate report ${report.id}:`, err.message);
                }
            }
        }
    } catch (err) {
        console.error('[CRON] Error processing scheduled reports:', err);
    }
}

module.exports = { init };
