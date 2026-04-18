const { AuditLog } = require('../models');

async function logAction(reqUser, { action, module, referenceId, referenceNumber, details, clientId }) {
  try {
    const cId = clientId || reqUser.clientId;
    await AuditLog.create({
      companyId: reqUser.companyId,
      clientId: cId || null,
      userId: reqUser.id,
      action,
      module,
      referenceId: String(referenceId),
      referenceNumber: referenceNumber || null,
      details: details ? (typeof details === 'object' ? details : { info: details }) : null,
      timestamp: new Date(),
    });
  } catch (err) {
    console.error('Audit logging failed:', err);
    // We don't throw here to avoid blocking the main business logic
  }
}

module.exports = { logAction };
