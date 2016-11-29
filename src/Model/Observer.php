<?php

class MageProfis_PartialReindex_Model_Observer
extends Mage_Core_Model_Abstract
{
    public function beforeProductSave($event)
    {
        $product = $event->getProduct(); 
        /* @var $product Mage_Catalog_Model_Product */
        if ($product instanceof Mage_Catalog_Model_Product)
        {
            if ($product->hasData('status')
                && $product->getData('status') != $product->getOrigData('status')
                && (int) $product->getData('status') == Mage_Catalog_Model_Product_Status::STATUS_ENABLED)
            {
                $this->_invalidateCategory();
            }
        }
    }

    /**
     * @mageEvent controller_action_predispatch_adminhtml_catalog_product_massStatus
     */
    public function preDispatchCatalogProductMassStatus($event)
    {
        $status = (int) Mage::app()->getRequest()->getParam('status');
        if ($status == Mage_Catalog_Model_Product_Status::STATUS_ENABLED)
        {
            $this->_invalidateCategory();
        }
    }

    /**
     * @mageEvent controller_action_predispatch_adminhtml_catalog_product_action_attribute_validate
     */
    public function preDispatchCatalogProductActionAttributeValidate($event)
    {
        $params = Mage::app()->getRequest()->getParams();
        if (isset($params['attributes']['status'])
            && (int) $params['attributes']['status'] == Mage_Catalog_Model_Product_Status::STATUS_ENABLED)
        {
            $this->_invalidateCategory();
        }
        
    }

    protected function _invalidateCategory()
    {
        $this->_connection('core_write')
                ->query("UPDATE ".$this->getTableName('index_process')." SET status = 'require_reindex' WHERE indexer_code = 'catalog_category_flat';");
    }

    /**
     * 
     * @return Mage_Core_Model_Resource
     */
    protected function _resource()
    {
        return Mage::getSingleton('core/resource');
    }

    /**
     * 
     * @return Varien_Db_Adapter_Interface
     */
    protected function _connection($type = 'core_read')
    {
        return $this->_resource()->getConnection($type);
    }

    /**
     * 
     * @param string $modelEntity
     * @return string
     */
    protected function getTableName($modelEntity)
    {
        return $this->_resource()->getTableName($modelEntity);
    }
}
