const { Product, OrderItem, SalesOrder, Category, sequelize } = require('../models');
const { Op } = require('sequelize');

/**
 * Marketplace pricing calculator – same channel config and formula as frontend
 */
const CHANNELS = [
  { value: 'Amazon_FBA', label: 'Amazon FBA', feePercent: 15, fulfillment: 3.5 },
  { value: 'Amazon_UK_FBA', label: 'Amazon UK FBA', feePercent: 15.3, fulfillment: 3.25 },
  { value: 'Amazon_UK_MFN', label: 'Amazon UK MFN', feePercent: 13.5, fulfillment: 0 },
  { value: 'Shopify', label: 'Shopify', feePercent: 2.9, fulfillment: 0 },
  { value: 'eBay', label: 'eBay', feePercent: 12.8, fulfillment: 0 },
  { value: 'TikTok', label: 'TikTok Shop', feePercent: 5, fulfillment: 0 },
  { value: 'Temu', label: 'Temu', feePercent: 8, fulfillment: 0 },
  { value: 'Direct', label: 'Direct / Website', feePercent: 0, fulfillment: 0 },
];

async function pricingCalculate(req, res, next) {
  try {
    const body = req.body || {};
    const productCost = Number(body.productCost) || 0;
    const packagingCost = Number(body.packagingCost) || 0;
    const shippingCost = Number(body.shippingCost) || 0;
    const laborCost = Number(body.laborCost) || 0;
    const desiredMargin = Number(body.desiredMargin) ?? 0.2;
    const channelType = body.channelType || 'Amazon_UK_FBA';

    const channelConfig = CHANNELS.find((c) => c.value === channelType) || CHANNELS[1];
    const feePercent = channelConfig.feePercent / 100;
    const fulfillmentFee = channelConfig.fulfillment;

    const baseCost = productCost + packagingCost + shippingCost + laborCost + fulfillmentFee;
    const recommendedSellingPrice = baseCost / (1 - desiredMargin - feePercent);
    const channelFee = recommendedSellingPrice * feePercent;
    const totalCost = baseCost + channelFee;
    const profit = recommendedSellingPrice - totalCost;
    const actualMargin = recommendedSellingPrice > 0 ? profit / recommendedSellingPrice : 0;

    const result = {
      productCost,
      consumablesCost: packagingCost,
      shippingCost,
      laborCost,
      fulfillmentFee,
      fees: channelFee + fulfillmentFee,
      totalCost,
      recommendedSellingPrice,
      profit,
      margin: actualMargin,
      breakdown: {
        channelFeePercent: channelConfig.feePercent,
        fulfillmentFee: channelConfig.fulfillment,
      },
    };

    res.json({ success: true, data: result });
  } catch (err) {
    next(err);
  }
}

/**
 * GET /api/analytics/margins – margin analysis with flow (channel, category)
 */
async function marginsReport(req, res, next) {
  try {
    const user = req.user;
    const companyId = user.companyId;
    const { channel, category } = req.query;

    if (!companyId && user.role !== 'super_admin') {
      return res.json({ success: true, data: { products: [], kpis: {}, topPerformers: [], bottomPerformers: [], channels: ['Direct'], categories: [] } });
    }

    const productWhere = {};
    if (companyId) productWhere.companyId = companyId;
    const categoryId = category && category !== 'All Categories' && category !== '' ? Number(category) : null;
    if (categoryId != null && !Number.isNaN(categoryId)) productWhere.categoryId = categoryId;

    const products = await Product.findAll({
      where: productWhere,
      include: [{ association: 'Category', attributes: ['id', 'name', 'code'], required: false }],
      order: [['name', 'ASC']],
    });

    const productIds = products.map((p) => p.id);
    if (productIds.length === 0) {
      const categories = await Category.findAll({ where: companyId ? { companyId } : {}, attributes: ['id', 'name'], raw: true });
      return res.json({
        success: true,
        data: {
          products: [],
          kpis: { avgGrossMargin: 0, totalNetProfit: 0, totalRevenue: 0, avgHealthScore: 0, improvementPotential: 0, lowMarginCount: 0 },
          topPerformers: [],
          bottomPerformers: [],
          channels: ['Direct'],
          categories: categories.map((c) => ({ id: c.id, name: c.name })),
        },
      });
    }

    let revenueByProduct = {};
    if (productIds.length > 0) {
      const dialect = sequelize.getDialect();
      const volCol = dialect === 'sqlite' ? 'SUM(oi.quantity)' : 'SUM(oi.quantity)';
      const revCol = dialect === 'sqlite' ? 'SUM(oi.quantity * oi.unit_price)' : 'SUM(oi.quantity * oi.unit_price)';
      const rows = await sequelize.query(
        `SELECT oi.product_id AS productId, ${volCol} AS volume, ${revCol} AS revenue
         FROM order_items oi
         INNER JOIN sales_orders so ON so.id = oi.sales_order_id
         WHERE so.company_id = :companyId AND oi.product_id IN (:productIds)
         GROUP BY oi.product_id`,
        { replacements: { companyId: companyId || 0, productIds }, type: sequelize.QueryTypes.SELECT }
      );
      const list = Array.isArray(rows) ? rows : [];
      list.forEach((row) => {
        revenueByProduct[row.productId] = { volume: Number(row.volume) || 0, revenue: Number(row.revenue) || 0 };
      });
    }

    const TARGET_MARGIN = 25;
    const LOW_MARGIN_THRESHOLD = 10;
    const productList = [];
    for (const p of products) {
      const plain = p.get ? p.get({ plain: true }) : p;
      const catName = plain.Category?.name || 'General';
      const sellingPrice = Number(plain.price) || 0;
      const productCost = Number(plain.costPrice) != null ? Number(plain.costPrice) : sellingPrice * 0.6;
      const volData = revenueByProduct[plain.id] || { volume: 0, revenue: 0 };
      const volume = volData.volume || 0;
      const totalRevenue = volData.revenue || sellingPrice * (volume || 1);
      const effectiveVolume = volume || 1;
      const totalCost = productCost * effectiveVolume;
      const grossProfit = totalRevenue - totalCost;
      const profitMargin = totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : (sellingPrice > 0 ? ((sellingPrice - productCost) / sellingPrice) * 100 : 0);
      const netProfit = grossProfit;
      const netMargin = totalRevenue > 0 ? (netProfit / totalRevenue) * 100 : profitMargin;

      let grade = 'D';
      if (profitMargin >= 30) grade = 'A';
      else if (profitMargin >= 20) grade = 'B';
      else if (profitMargin >= 10) grade = 'C';

      const healthScore = Math.min(100, Math.max(-100, profitMargin * 2));

      const improvementPotential = profitMargin < LOW_MARGIN_THRESHOLD && totalRevenue > 0 ? (TARGET_MARGIN / 100 - profitMargin / 100) * totalRevenue : 0;

      productList.push({
        id: plain.id,
        sku: plain.sku || `SKU-${plain.id}`,
        name: plain.name,
        brand: 'In-House',
        channel: 'Direct',
        category: catName,
        categoryId: plain.categoryId,
        sellingPrice,
        productCost,
        totalRevenue,
        volume,
        totalCost,
        grossProfit,
        profitMargin,
        netProfit,
        netMargin,
        grade,
        healthScore,
        improvementPotential,
      });
    }

    const filtered = (channel && channel !== 'All Channels' && channel !== '')
      ? productList.filter((r) => r.channel === channel)
      : productList;

    const totalRevenue = filtered.reduce((s, p) => s + p.totalRevenue, 0);
    const totalNetProfit = filtered.reduce((s, p) => s + p.netProfit, 0);
    const avgGrossMargin = filtered.length ? filtered.reduce((s, p) => s + p.profitMargin, 0) / filtered.length : 0;
    const avgHealthScore = filtered.length ? filtered.reduce((s, p) => s + p.healthScore, 0) / filtered.length : 0;
    const improvementPotential = filtered.reduce((s, p) => s + (p.improvementPotential || 0), 0);
    const lowMarginCount = filtered.filter((p) => p.profitMargin < LOW_MARGIN_THRESHOLD).length;

    const topPerformers = [...filtered].sort((a, b) => b.netProfit - a.netProfit).slice(0, 3);
    const bottomPerformers = [...filtered].sort((a, b) => a.profitMargin - b.profitMargin).slice(0, 3);

    const categories = await Category.findAll({ where: companyId ? { companyId } : {}, attributes: ['id', 'name'], raw: true });

    res.json({
      success: true,
      data: {
        products: filtered,
        kpis: {
          avgGrossMargin,
          totalNetProfit,
          totalRevenue,
          avgHealthScore,
          improvementPotential,
          lowMarginCount,
          targetMargin: TARGET_MARGIN,
        },
        topPerformers,
        bottomPerformers,
        channels: ['Direct'],
        categories: categories.map((c) => ({ id: c.id, name: c.name })),
      },
    });
  } catch (err) {
    next(err);
  }
}

module.exports = { pricingCalculate, marginsReport };
