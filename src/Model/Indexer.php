<?php

class MageProfis_PartialReindex_Model_Indexer
{
    public function reindexEveryHour()
    {
        $collection = Mage::getModel('cron/schedule')->getCollection()
                        ->addFieldToFilter('status', Mage_Cron_Model_Schedule::STATUS_RUNNING)
                        ->addFieldToFilter('job_code', 'mp_indexer_link');
        if($collection->getSize() > 1)
        {
            return 'Process already running';
        }
        $time = strtotime(Mage::getSingleton('index/indexer')->getProcessByCode('catalog_category_product')->getStartedAt());
        if ($time < strtotime('-1 month'))
        {
            Mage::getSingleton('index/indexer')->getProcessByCode('catalog_category_product')->reindexEverything();
        }
    }

    public function reindexProducts()
    {
        $dynamiccategory = Mage::getModel('index/indexer')->getProcessByCode('dynamiccategory');
        $reindexed_products = false;
        if (Mage::getModel('index/indexer')->getProcessByCode('catalog_category_flat')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX
           || ($dynamiccategory && $dynamiccategory->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX))
        {
            Mage::getSingleton('index/indexer')->getProcessByCode('catalog_category_flat')->reindexEverything();
            Mage::getSingleton('index/indexer')->getProcessByCode('catalog_category_product')->reindexEverything();
            $reindexed_products = true;
        }

        $date = date('Y-m-d H:i:s', strtotime('-17 minutes'));
        $ids = array();

        $requests = array(
            "SELECT entity_id, updated_at FROM {$this->getTableName('catalog_product_entity')} WHERE updated_at >= '{$date}';",
            "SELECT product_id, created_at FROM {$this->getTableName('sales_flat_order_item')} WHERE created_at >= '{$date}';",
            "SELECT entity_pk FROM {$this->getTableName('index_event')} WHERE entity = 'cataloginventory_stock_item';",
            "SELECT entity_pk FROM {$this->getTableName('index_event')} WHERE entity = 'catalog_product';"
        );
        
        foreach ($requests as $sql)
        {
            foreach ($this->_connection()->fetchCol($sql) as $_id)
            {
                $ids[] = (int)$_id;
            }
        }
        $ids = array_unique($ids);
        
        if (count($ids) == 0)
        {
            return $this;
        }
        $event = Mage::getModel('index/event');
        $event->setNewData(array(
            'reindex_price_product_ids' => &$ids, // for product_indexer_price
            'reindex_stock_product_ids' => &$ids, // for indexer_stock
            'product_ids'               => &$ids, // for category_indexer_product
            'reindex_eav_product_ids'   => &$ids  // for product_indexer_eav
        ));

        if (Mage::getModel('index/indexer')->getProcessByCode('cataloginventory_stock')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX)
        {
            $this->_connection()
                    ->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'cataloginventory_stock\'');
            Mage::getResourceSingleton('cataloginventory/indexer_stock')->catalogProductMassAction($event);
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'cataloginventory_stock\'');
        }

        if (Mage::getModel('index/indexer')->getProcessByCode('catalog_product_price')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX)
        {
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_price\'');
            Mage::getResourceSingleton('catalog/product_indexer_price')->catalogProductMassAction($event);
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_price\'');
        }

        if (Mage::getModel('index/indexer')->getProcessByCode('catalog_product_attribute')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX)
        {
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_attribute\'');
            Mage::getResourceSingleton('catalog/product_indexer_eav')->catalogProductMassAction($event);
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_attribute\'');
        }

        if (!$reindexed_products && Mage::getModel('index/indexer')->getProcessByCode('catalog_category_product')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX)
        {
            $collection = Mage::getModel('cron/schedule')->getCollection()
                            ->addFieldToFilter('status', Mage_Cron_Model_Schedule::STATUS_RUNNING)
                            ->addFieldToFilter('job_code', 'mp_indexer_link');
            if($collection->getSize() > 0)
            {
                $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_category_product\'');
                Mage::getResourceSingleton('catalog/category_indexer_product')->catalogProductMassAction($event);
                $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_category_product\'');
            }
        }

        if (Mage::helper('catalog/product_flat')->isEnabled() && Mage::getModel('index/indexer')->getProcessByCode('catalog_product_flat')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX) {
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_flat\'');
            Mage::getSingleton('catalog/product_flat_indexer')->saveProduct($ids);
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_product_flat\'');
        }
        
        if (Mage::getModel('index/indexer')->getProcessByCode('catalog_url')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX) {
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_url\'');
            /* @var $urlModel Mage_Catalog_Model_Url */
            $urlModel = Mage::getSingleton('catalog/url');
            $urlModel->clearStoreInvalidRewrites();
            foreach ($ids as $productId) {
                $urlModel->refreshProductRewrite($productId);
            }
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalog_url\'');
        }

        if (Mage::getModel('index/indexer')->getProcessByCode('catalogsearch_fulltext')->getStatus() == Mage_Index_Model_Process::STATUS_REQUIRE_REINDEX) {
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalogsearch_fulltext\'');
            if (Mage::getStoreConfigFlag('dev/mp_indexer/enable_fulltext'))
            {
                Mage::getResourceSingleton('catalogsearch/fulltext')->rebuildIndex(null, $ids);
            }
            $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'catalogsearch_fulltext\'');
        }
        
        // mark this everytime as correct!
        $this->_connection()->update($this->getTableName('index_process'), array('status' => 'working', 'started_at' => date('Y-m-d H:i:s')), 'indexer_code=\'tag_summary\'');
        $this->_connection()->update($this->getTableName('index_process'), array('status' => 'pending', 'ended_at' => date('Y-m-d H:i:s')), 'indexer_code=\'tag_summary\'');

        // remove index info, from this products
        $where = "(entity = 'catalog_product' OR entity = 'cataloginventory_stock_item' OR entity = 'catalogsearch_fulltext' OR entity = 'tag_summary') AND ";
        $where .= $this->_connection()->quoteInto('entity_pk IN(?)', $ids);
        $this->_connection()->delete($this->getTableName('index_event'), $where);

        $where = "(entity = 'catalog_product' OR entity = 'cataloginventory_stock_item' OR entity = 'catalogsearch_fulltext' OR entity = 'tag_summary') AND ";
        $where .= $this->_connection()->quoteInto('entity_pk IS NULL', null);
        $this->_connection()->delete($this->getTableName('index_event'), $where);
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
