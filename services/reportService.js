const { Report, Product, Warehouse, Location, SalesOrder, Customer, OrderItem, Batch } = require('../models');
const { Op } = require('sequelize');
const PDFDocument = require('pdfkit');

async function list(reqUser, query = {}) {
  const where = {};
  if (reqUser.role !== 'super_admin') where.companyId = reqUser.companyId;
  else if (query.companyId) where.companyId = query.companyId;
  if (query.category) where.category = query.category;
  if (query.reportType) where.reportType = query.reportType;
  if (query.format) where.format = query.format;
  if (query.schedule) where.schedule = query.schedule;
  if (query.status) where.status = query.status;
  if (query.startDate || query.endDate) {
    where.lastRunAt = {};
    if (query.startDate) where.lastRunAt[Op.gte] = new Date(query.startDate);
    if (query.endDate) where.lastRunAt[Op.lte] = new Date(query.endDate + 'T23:59:59.999Z');
  }
  if (query.search) {
    where[Op.or] = [
      { reportName: { [Op.like]: `%${query.search}%` } },
      { category: { [Op.like]: `%${query.search}%` } },
    ];
  }

  const reports = await Report.findAll({
    where,
    order: [['createdAt', 'DESC']],
    attributes: { exclude: ['content'] } // Exclude large content from list
  });
  return reports.map((r) => r.get({ plain: true }));
}

async function getById(id, reqUser) {
  const report = await Report.findByPk(id);
  if (!report) throw new Error('Report not found');
  if (reqUser.role !== 'super_admin' && report.companyId !== reqUser.companyId) throw new Error('Report not found');
  return report.get({ plain: true });
}

async function update(id, data, reqUser) {
  const report = await Report.findByPk(id);
  if (!report) throw new Error('Report not found');
  if (reqUser.role !== 'super_admin' && report.companyId !== reqUser.companyId) throw new Error('Report not found');
  const updates = {};
  if (data.reportName != null) updates.reportName = data.reportName;
  if (data.reportType != null) updates.reportType = data.reportType;
  if (data.category != null) updates.category = data.category;
  if (data.startDate != null) updates.startDate = data.startDate;
  if (data.endDate != null) updates.endDate = data.endDate;
  if (data.format != null) updates.format = data.format;
  if (data.schedule != null) updates.schedule = data.schedule;
  if (data.status != null) updates.status = data.status;
  await report.update(updates);
  return report.get({ plain: true });
}

async function create(data, reqUser) {
  const companyId = reqUser.companyId;
  if (!companyId && reqUser.role !== 'super_admin') throw new Error('companyId required');

  const reportType = (data.reportType || data.category || 'INVENTORY').toUpperCase().replace(/\s+/g, '_');
  const validTypes = ['INVENTORY', 'ORDERS', 'FINANCIAL', 'PERFORMANCE', 'EXPIRY_WARNING'];
  const type = validTypes.includes(reportType) ? reportType : 'INVENTORY';

  const startDate = data.startDate ? (data.startDate.split('T')[0] || data.startDate) : null;
  const endDate = data.endDate ? (data.endDate.split('T')[0] || data.endDate) : null;

  let content = '';
  let headers = [];
  let rows = [];
  let stocks = null;
  let orders = null;
  let sortedStats = null;
  let expiryBatches = null;

  // Generate Content based on Type
  if (type === 'INVENTORY') {
    // Fetch Inventory Data
    const { ProductStock } = require('../models');
    stocks = await ProductStock.findAll({
      where: companyId ? { '$Warehouse.companyId$': companyId } : {},
      include: [
        { model: Product, attributes: ['name', 'sku'] },
        { model: Warehouse, attributes: ['name', 'companyId'], where: companyId ? { companyId } : {} },
        { model: Location, attributes: ['name', 'code'] }
      ]
    });

    headers = ['Product Name', 'SKU', 'Batch Number', 'Expiry Date', 'Warehouse', 'Location', 'Quantity', 'Reserved', 'Available', 'Status', 'Last Updated'];
    rows = stocks.map(s => {
      const prodName = s.Product?.name || '';
      const sku = s.Product?.sku || '';
      const batch = s.batchNumber || '-';
      const expiry = s.bestBeforeDate || '-';
      const whName = s.Warehouse?.name || '';
      const locName = s.Location?.name || s.Location?.code || '';
      const qty = s.quantity || 0;
      const rsv = s.reserved || 0;
      const avail = Math.max(0, qty - rsv);
      const status = s.status || 'ACTIVE';
      const updated = s.updatedAt ? new Date(s.updatedAt).toISOString() : '';
      return `"${prodName}","${sku}","${batch}","${expiry}","${whName}","${locName}",${qty},${rsv},${avail},"${status}","${updated}"`;
    });
    content = [headers.join(','), ...rows].join('\n');
  }
  else if (type === 'ORDERS') {
    const where = companyId ? { companyId } : {};
    if (startDate) where.createdAt = { [Op.gte]: new Date(startDate) };
    if (endDate) {
      if (!where.createdAt) where.createdAt = {};
      where.createdAt[Op.lte] = new Date(endDate + 'T23:59:59.999Z');
    }

    orders = await SalesOrder.findAll({
      where,
      include: [
        { model: Customer, attributes: ['name', 'email'] },
        { model: OrderItem, include: [{ model: Product, attributes: ['sku'] }] }
      ]
    });

    headers = ['Order Number', 'Customer', 'Date', 'Status', 'Total', 'Items'];
    rows = orders.map(o => {
      const orderNum = o.orderNumber || '';
      const custName = o.Customer?.name || '';
      const date = o.createdAt ? new Date(o.createdAt).toISOString() : '';
      const status = o.status || '';
      const total = o.totalAmount || 0;
      const items = (o.OrderItems || []).map(i => `${i.Product?.sku || 'sku'} (x${i.quantity})`).join('; ');
      return `"${orderNum}","${custName}","${date}","${status}",${total},"${items}"`;
    });
    content = [headers.join(','), ...rows].join('\n');
  }
  else if (type === 'PERFORMANCE') {
    // Top Selling Products
    const where = {};
    if (startDate) where.createdAt = { [Op.gte]: new Date(startDate) };
    if (endDate) {
      if (!where.createdAt) where.createdAt = {};
      where.createdAt[Op.lte] = new Date(endDate + 'T23:59:59.999Z');
    }

    // Fetch all items from sales orders with status CONFIRMED or above
    const items = await OrderItem.findAll({
      include: [
        {
          model: SalesOrder,
          where: {
            ...where,
            status: { [Op.notIn]: ['DRAFT', 'CANCELLED'] },
            ...(companyId ? { companyId } : {})
          },
          attributes: []
        },
        { model: Product, attributes: ['name', 'sku'] }
      ]
    });

    // Aggregate in memory
    const productStats = {};
    items.forEach(item => {
      const pid = item.productId;
      if (!productStats[pid]) {
        productStats[pid] = {
          name: item.Product?.name || 'Unknown',
          sku: item.Product?.sku || 'sku',
          qty: 0,
          revenue: 0
        };
      }
      productStats[pid].qty += (item.quantity || 0);
      productStats[pid].revenue += (Number(item.subtotal) || 0);
    });

    sortedStats = Object.values(productStats).sort((a, b) => b.qty - a.qty).slice(0, 50);

    headers = ['Rank', 'Product Name', 'SKU', 'Total Quantity Sold', 'Total Revenue'];
    rows = sortedStats.map((s, idx) => {
      return `${idx + 1},"${s.name}","${s.sku}",${s.qty},${s.revenue.toFixed(2)}`;
    });
    content = [headers.join(','), ...rows].join('\n');
  }
  else if (type === 'EXPIRY_WARNING') {
    const batches = await Batch.findAll({
      where: {
        expiryDate: { [Op.ne]: null },
        status: 'ACTIVE',
        ...(companyId ? { companyId } : {})
      },
      include: [
        { model: Product, attributes: ['name', 'sku', 'bestBeforeDateWarningPeriodDays'] },
        { model: Warehouse, attributes: ['name'] },
        { model: Location, attributes: ['name', 'code'] }
      ]
    });

    const now = new Date();
    expiryBatches = batches.filter(b => {
      const warningDays = b.Product?.bestBeforeDateWarningPeriodDays || 0;
      if (warningDays <= 0) return false;
      
      const expiryDate = new Date(b.expiryDate);
      const diffTime = expiryDate.getTime() - now.getTime();
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      
      return diffDays <= warningDays;
    });

    headers = ['Product Name', 'SKU', 'Batch Number', 'Warehouse', 'Location', 'Quantity', 'Expiry Date', 'Days Left'];
    rows = expiryBatches.map(b => {
      const prodName = b.Product?.name || '';
      const sku = b.Product?.sku || '';
      const batchNum = b.batchNumber || '';
      const whName = b.Warehouse?.name || '';
      const locName = b.Location?.name || b.Location?.code || '';
      const qty = b.quantity || 0;
      const expiry = b.expiryDate || '';
      
      const expiryDate = new Date(b.expiryDate);
      const diffTime = expiryDate.getTime() - now.getTime();
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      
      return `"${prodName}","${sku}","${batchNum}","${whName}","${locName}",${qty},"${expiry}",${diffDays}`;
    });
    content = [headers.join(','), ...rows].join('\n');
  }

  const headersStr = headers.join(',');

  const reportData = {
    companyId: companyId || data.companyId,
    reportName: data.reportName || `Report ${type} ${new Date().toISOString().slice(0, 10)}`,
    reportType: type,
    category: type,
    startDate,
    endDate,
    format: (data.format || 'CSV').toUpperCase(),
    schedule: (data.schedule || 'ONCE').toUpperCase(),
    status: 'COMPLETED',
    lastRunAt: new Date(),
  };

  if (reportData.format === 'PDF') {
    const pdfBuffer = await generatePDFBuffer(reportData.reportName, headers, stocks || orders || sortedStats || expiryBatches, type);
    reportData.content = pdfBuffer.toString('base64');
  } else {
    reportData.content = [headersStr, ...rows].join('\n');
  }

  const report = await Report.create(reportData);

  const plain = report.get({ plain: true });
  delete plain.content; // Don't send content in list/create response to keep it light
  return plain;
}

async function remove(id, reqUser) {
  const report = await Report.findByPk(id);
  if (!report) throw new Error('Report not found');
  if (reqUser.role !== 'super_admin' && report.companyId !== reqUser.companyId) throw new Error('Report not found');
  await report.destroy();
  return { message: 'Report deleted' };
}

async function download(id, reqUser) {
  const report = await Report.findByPk(id);
  if (!report) throw new Error('Report not found');
  if (reqUser.role !== 'super_admin' && report.companyId !== reqUser.companyId) throw new Error('Report not found');
  if (!report.content) throw new Error('Report content not available');

  if (report.format === 'PDF') {
    return {
      filename: `${report.reportName.replace(/[^a-z0-9]/gi, '_').toLowerCase()}.pdf`,
      content: Buffer.from(report.content, 'base64'),
      mimeType: 'application/pdf'
    };
  }

  return {
    filename: `${report.reportName.replace(/[^a-z0-9]/gi, '_').toLowerCase()}.csv`,
    content: report.content,
    mimeType: 'text/csv'
  };
}

async function generatePDFBuffer(title, headers, data, type) {
  return new Promise((resolve) => {
    const doc = new PDFDocument({ margin: 30, size: 'A4' });
    const buffers = [];
    doc.on('data', buffers.push.bind(buffers));
    doc.on('end', () => resolve(Buffer.concat(buffers)));

    // Header
    doc.fontSize(20).text('WMS PROFESSIONAL REPORT', { align: 'center' });
    doc.fontSize(14).text(title, { align: 'center' });
    doc.fontSize(10).text(`Generated on: ${new Date().toLocaleString()}`, { align: 'center' });
    doc.moveDown(2);

    // Dynamic Table Generation
    const startX = 30;
    let currentY = doc.y;
    const colWidth = (595 - 60) / headers.length;

    // Draw Headers
    doc.fontSize(10).fillColor('#444444');
    headers.forEach((h, i) => {
      doc.text(h, startX + (i * colWidth), currentY, { width: colWidth, align: 'left' });
    });

    doc.moveTo(startX, currentY + 15).lineTo(565, currentY + 15).stroke();
    currentY += 25;

    // Draw Rows
    doc.fontSize(9).fillColor('#000000');
    data.forEach((item, rowIndex) => {
      if (currentY > 750) {
        doc.addPage();
        currentY = 30;
      }

      let rowValues = [];
      if (type === 'INVENTORY') {
        rowValues = [
          item.Product?.name || '',
          item.Product?.sku || '',
          item.batchNumber || '-',
          item.bestBeforeDate || '-',
          item.Warehouse?.name || '',
          item.Location?.name || item.Location?.code || '',
          String(item.quantity || 0),
          String(item.reserved || 0),
          String(Math.max(0, item.quantity - item.reserved)),
          item.status || '',
          item.updatedAt ? new Date(item.updatedAt).toLocaleDateString() : ''
        ];
      } else if (type === 'ORDERS') {
        rowValues = [
          item.orderNumber || '',
          item.Customer?.name || '',
          item.createdAt ? new Date(item.createdAt).toLocaleDateString() : '',
          item.status || '',
          String(item.totalAmount || 0),
          (item.OrderItems || []).map(i => i.Product?.sku).join(', ')
        ];
      } else if (type === 'PERFORMANCE') {
        // Here item is the aggregated object from sortedStats
        rowValues = [
          String(rowIndex + 1),
          item.name,
          item.sku,
          String(item.qty),
          String(item.revenue.toFixed(2))
        ];
      } else if (type === 'EXPIRY_WARNING') {
        const now = new Date();
        const expiryDate = new Date(item.expiryDate);
        const diffTime = expiryDate.getTime() - now.getTime();
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

        rowValues = [
          item.Product?.name || '',
          item.Product?.sku || '',
          item.batchNumber || '',
          item.Warehouse?.name || '',
          item.Location?.name || item.Location?.code || '',
          String(item.quantity || 0),
          item.expiryDate || '',
          String(diffDays)
        ];
      }

      rowValues.forEach((val, i) => {
        doc.text(String(val).substring(0, 25), startX + (i * colWidth), currentY, { width: colWidth, align: 'left' });
      });

      currentY += 20;
    });

    doc.end();
  });
}

module.exports = { list, getById, create, update, remove, download };
