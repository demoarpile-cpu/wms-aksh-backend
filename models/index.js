const { sequelize } = require('../config/db');
const User = require('./User');
const Company = require('./Company');
const Warehouse = require('./Warehouse');
const Zone = require('./Zone');
const Location = require('./Location');
const Category = require('./Category');
const Product = require('./Product');
const ProductStock = require('./ProductStock');
const Bundle = require('./Bundle');
const BundleItem = require('./BundleItem');
const Customer = require('./Customer');
const Supplier = require('./Supplier');
const SalesOrder = require('./SalesOrder');
const OrderItem = require('./OrderItem');
const PickList = require('./PickList');
const PickListItem = require('./PickListItem');
const PackingTask = require('./PackingTask');
const Shipment = require('./Shipment');
const PurchaseOrder = require('./PurchaseOrder');
const PurchaseOrderItem = require('./PurchaseOrderItem');
const GoodsReceipt = require('./GoodsReceipt');
const GoodsReceiptItem = require('./GoodsReceiptItem');
const InventoryAdjustment = require('./InventoryAdjustment');
const CycleCount = require('./CycleCount');
const Batch = require('./Batch');
const Movement = require('./Movement');
const ReplenishmentTask = require('./ReplenishmentTask');
const ReplenishmentConfig = require('./ReplenishmentConfig');
const Report = require('./Report');
const Return = require('./Return');
const VatCode = require('./VatCode');
const Inventory = require('./Inventory');
const InventoryLog = require('./InventoryLog');
const SupplierProduct = require('./SupplierProduct');
const AuditLog = require('./AuditLog');


// Company
Company.hasMany(User, { foreignKey: 'companyId' });
User.belongsTo(Company, { foreignKey: 'companyId' });

// User -> Warehouse (staff can be assigned to a warehouse)
Warehouse.hasMany(User, { foreignKey: 'warehouseId' });
User.belongsTo(Warehouse, { foreignKey: 'warehouseId' });

// Product <-> Category (explicit alias so include 'Category' / 'Products' works)
Category.hasMany(Product, { foreignKey: 'categoryId', as: 'Products' });
Product.belongsTo(Category, { foreignKey: 'categoryId', as: 'Category' });

// Company -> Warehouse, Category, Product, Customer, Supplier, SalesOrder, PurchaseOrder, etc.
Company.hasMany(Warehouse, { foreignKey: 'companyId' });
Warehouse.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(Category, { foreignKey: 'companyId' });
Category.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(Product, { foreignKey: 'companyId' });
Product.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(Customer, { foreignKey: 'companyId' });
Customer.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(Supplier, { foreignKey: 'companyId' });
Supplier.belongsTo(Company, { foreignKey: 'companyId' });
SupplierProduct.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(SupplierProduct, { foreignKey: 'companyId' });
Company.hasMany(SalesOrder, { foreignKey: 'companyId' });
SalesOrder.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(PurchaseOrder, { foreignKey: 'companyId' });
PurchaseOrder.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(GoodsReceipt, { foreignKey: 'companyId' });
GoodsReceipt.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(VatCode, { foreignKey: 'companyId' });
VatCode.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(Zone, { foreignKey: 'companyId' });
Zone.belongsTo(Company, { foreignKey: 'companyId' });
Company.hasMany(AuditLog, { foreignKey: 'companyId' });
AuditLog.belongsTo(Company, { foreignKey: 'companyId' });
AuditLog.belongsTo(User, { foreignKey: 'userId', as: 'User' });

// Product -> Supplier
Supplier.hasMany(Product, { foreignKey: 'supplierId' });
Product.belongsTo(Supplier, { foreignKey: 'supplierId' });

// SupplierProduct mappings
Supplier.hasMany(SupplierProduct, { foreignKey: 'supplierId', as: 'SupplierProducts' });
SupplierProduct.belongsTo(Supplier, { foreignKey: 'supplierId' });
Product.hasMany(SupplierProduct, { foreignKey: 'productId', as: 'SupplierProducts' });
SupplierProduct.belongsTo(Product, { foreignKey: 'productId' });

// Warehouse -> Zone -> Location
Zone.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
Warehouse.hasMany(Zone, { foreignKey: 'warehouseId' });
Zone.hasMany(Location, { foreignKey: 'zoneId' });
Location.belongsTo(Zone, { foreignKey: 'zoneId' });

// SalesOrder -> OrderItem, PickList, PackingTask, Shipment, Customer
Customer.hasMany(SalesOrder, { foreignKey: 'customerId' });
SalesOrder.belongsTo(Customer, { foreignKey: 'customerId' });
SalesOrder.hasMany(OrderItem, { foreignKey: 'salesOrderId' });
OrderItem.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
OrderItem.belongsTo(Product, { foreignKey: 'productId' });
Product.hasMany(OrderItem, { foreignKey: 'productId' });

SalesOrder.hasMany(PickList, { foreignKey: 'salesOrderId' });
PickList.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
PickList.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
Warehouse.hasMany(PickList, { foreignKey: 'warehouseId' });
PickList.belongsTo(User, { foreignKey: 'assignedTo', as: 'User' });
User.hasMany(PickList, { foreignKey: 'assignedTo' });
PickList.hasMany(PickListItem, { foreignKey: 'pickListId' });
PickListItem.belongsTo(PickList, { foreignKey: 'pickListId' });
PickListItem.belongsTo(Product, { foreignKey: 'productId' });
Product.hasMany(PickListItem, { foreignKey: 'productId' });

SalesOrder.hasMany(PackingTask, { foreignKey: 'salesOrderId' });
PackingTask.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
PackingTask.belongsTo(PickList, { foreignKey: 'pickListId' });
PickList.hasMany(PackingTask, { foreignKey: 'pickListId' });
PackingTask.belongsTo(User, { foreignKey: 'assignedTo', as: 'User' });
User.hasMany(PackingTask, { foreignKey: 'assignedTo' });

SalesOrder.hasOne(Shipment, { foreignKey: 'salesOrderId' });
Shipment.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
Shipment.belongsTo(Company, { foreignKey: 'companyId' });
Shipment.belongsTo(User, { foreignKey: 'packedBy', as: 'User' });
User.hasMany(Shipment, { foreignKey: 'packedBy' });

// PurchaseOrder -> PurchaseOrderItem, Supplier, Warehouse
PurchaseOrder.belongsTo(Supplier, { foreignKey: 'supplierId' });
Supplier.hasMany(PurchaseOrder, { foreignKey: 'supplierId' });
PurchaseOrder.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
Warehouse.hasMany(PurchaseOrder, { foreignKey: 'warehouseId' });
PurchaseOrder.belongsTo(Customer, { foreignKey: 'clientId', as: 'Client' });
Customer.hasMany(PurchaseOrder, { foreignKey: 'clientId' });
PurchaseOrder.hasMany(PurchaseOrderItem, { foreignKey: 'purchaseOrderId' });

PurchaseOrderItem.belongsTo(PurchaseOrder, { foreignKey: 'purchaseOrderId' });
PurchaseOrderItem.belongsTo(Product, { foreignKey: 'productId' });

// GoodsReceipt -> GoodsReceiptItem
GoodsReceipt.belongsTo(PurchaseOrder, { foreignKey: 'purchaseOrderId' });
PurchaseOrder.hasMany(GoodsReceipt, { foreignKey: 'purchaseOrderId' });
GoodsReceipt.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
Warehouse.hasMany(GoodsReceipt, { foreignKey: 'warehouseId' });
GoodsReceipt.hasMany(GoodsReceiptItem, { foreignKey: 'goodsReceiptId' });
GoodsReceiptItem.belongsTo(GoodsReceipt, { foreignKey: 'goodsReceiptId' });
GoodsReceiptItem.belongsTo(Product, { foreignKey: 'productId' });

// ProductStock
Product.hasMany(ProductStock, { foreignKey: 'productId' });
ProductStock.belongsTo(Product, { foreignKey: 'productId' });
Warehouse.hasMany(ProductStock, { foreignKey: 'warehouseId' });
ProductStock.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
ProductStock.belongsTo(Location, { foreignKey: 'locationId' });
ProductStock.belongsTo(Customer, { foreignKey: 'clientId', as: 'Client' });

// InventoryAdjustment (createdBy -> User as createdByUser)
Product.hasMany(InventoryAdjustment, { foreignKey: 'productId' });
InventoryAdjustment.belongsTo(Product, { foreignKey: 'productId' });
Warehouse.hasMany(InventoryAdjustment, { foreignKey: 'warehouseId' });
InventoryAdjustment.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
User.hasMany(InventoryAdjustment, { foreignKey: 'createdBy', as: 'inventoryAdjustmentsCreated' });
InventoryAdjustment.belongsTo(User, { foreignKey: 'createdBy', as: 'createdByUser' });
InventoryAdjustment.belongsTo(Location, { foreignKey: 'locationId', as: 'Location' });
InventoryAdjustment.belongsTo(Customer, { foreignKey: 'clientId', as: 'Client' });

// CycleCount (countedBy -> User as countedByUser)
Location.hasMany(CycleCount, { foreignKey: 'locationId' });
CycleCount.belongsTo(Location, { foreignKey: 'locationId' });
User.hasMany(CycleCount, { foreignKey: 'countedBy', as: 'countedByUser' });
CycleCount.belongsTo(User, { foreignKey: 'countedBy', as: 'countedByUser' });

// Batch
Product.hasMany(Batch, { foreignKey: 'productId' });
Batch.belongsTo(Product, { foreignKey: 'productId' });
Warehouse.hasMany(Batch, { foreignKey: 'warehouseId' });
Batch.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
Location.hasMany(Batch, { foreignKey: 'locationId' });
Batch.belongsTo(Location, { foreignKey: 'locationId' });
Supplier.hasMany(Batch, { foreignKey: 'supplierId' });
Batch.belongsTo(Supplier, { foreignKey: 'supplierId' });

// Movement (fromLocation, toLocation, createdByUser, warehouses)
Product.hasMany(Movement, { foreignKey: 'productId' });
Movement.belongsTo(Product, { foreignKey: 'productId' });
Batch.hasMany(Movement, { foreignKey: 'batchId' });
Movement.belongsTo(Batch, { foreignKey: 'batchId' });
Location.hasMany(Movement, { foreignKey: 'fromLocationId', as: 'movementsFrom' });
Movement.belongsTo(Location, { foreignKey: 'fromLocationId', as: 'fromLocation' });
Location.hasMany(Movement, { foreignKey: 'toLocationId', as: 'movementsTo' });
Movement.belongsTo(Location, { foreignKey: 'toLocationId', as: 'toLocation' });
Warehouse.hasMany(Movement, { foreignKey: 'fromWarehouseId', as: 'movementsFromWh' });
Movement.belongsTo(Warehouse, { foreignKey: 'fromWarehouseId', as: 'fromWarehouse' });
Warehouse.hasMany(Movement, { foreignKey: 'toWarehouseId', as: 'movementsToWh' });
Movement.belongsTo(Warehouse, { foreignKey: 'toWarehouseId', as: 'toWarehouse' });
User.hasMany(Movement, { foreignKey: 'createdBy', as: 'movementsCreated' });
Movement.belongsTo(User, { foreignKey: 'createdBy', as: 'createdByUser' });


// ReplenishmentTask (fromLocation, toLocation)
Product.hasMany(ReplenishmentTask, { foreignKey: 'productId' });
ReplenishmentTask.belongsTo(Product, { foreignKey: 'productId' });
Location.hasMany(ReplenishmentTask, { foreignKey: 'fromLocationId', as: 'replenishmentTasksFrom' });
ReplenishmentTask.belongsTo(Location, { foreignKey: 'fromLocationId', as: 'fromLocation' });
Location.hasMany(ReplenishmentTask, { foreignKey: 'toLocationId', as: 'replenishmentTasksTo' });
ReplenishmentTask.belongsTo(Location, { foreignKey: 'toLocationId', as: 'toLocation' });

// ReplenishmentConfig
Product.hasMany(ReplenishmentConfig, { foreignKey: 'productId' });
ReplenishmentConfig.belongsTo(Product, { foreignKey: 'productId' });

// Inventory
Product.hasMany(Inventory, { foreignKey: 'productId' });
Inventory.belongsTo(Product, { foreignKey: 'productId' });
Warehouse.hasMany(Inventory, { foreignKey: 'warehouseId' });
Inventory.belongsTo(Warehouse, { foreignKey: 'warehouseId' });

// InventoryLog
Product.hasMany(InventoryLog, { foreignKey: 'productId' });
InventoryLog.belongsTo(Product, { foreignKey: 'productId' });
Warehouse.hasMany(InventoryLog, { foreignKey: 'warehouseId' });
InventoryLog.belongsTo(Warehouse, { foreignKey: 'warehouseId' });
InventoryLog.belongsTo(Location, { foreignKey: 'locationId', as: 'Location' });
InventoryLog.belongsTo(Customer, { foreignKey: 'clientId', as: 'Client' });
InventoryLog.belongsTo(User, { foreignKey: 'userId', as: 'User' });


// Bundle -> BundleItem
Bundle.hasMany(BundleItem, { foreignKey: 'bundleId' });
BundleItem.belongsTo(Bundle, { foreignKey: 'bundleId' });
BundleItem.belongsTo(Product, { foreignKey: 'productId' });
Product.hasMany(BundleItem, { foreignKey: 'productId' });

// Returns
Company.hasMany(Return, { foreignKey: 'companyId' });
Return.belongsTo(Company, { foreignKey: 'companyId' });
SalesOrder.hasMany(Return, { foreignKey: 'salesOrderId' });
Return.belongsTo(SalesOrder, { foreignKey: 'salesOrderId' });
Shipment.hasMany(Return, { foreignKey: 'shipmentId' });
Return.belongsTo(Shipment, { foreignKey: 'shipmentId' });
Customer.hasMany(Return, { foreignKey: 'customerId' });
Return.belongsTo(Customer, { foreignKey: 'customerId' });

module.exports = {
  sequelize,
  User,
  Company,
  Warehouse,
  Zone,
  Location,
  Category,
  Product,
  ProductStock,
  Bundle,
  BundleItem,
  Customer,
  Supplier,
  SalesOrder,
  OrderItem,
  PickList,
  PickListItem,
  PackingTask,
  Shipment,
  PurchaseOrder,
  PurchaseOrderItem,
  GoodsReceipt,
  GoodsReceiptItem,
  InventoryAdjustment,
  CycleCount,
  Batch,
  Movement,
  ReplenishmentTask,
  ReplenishmentConfig,
  Report,
  Return,
  VatCode,
  Inventory,
  InventoryLog,
  SupplierProduct,
  AuditLog,
};
