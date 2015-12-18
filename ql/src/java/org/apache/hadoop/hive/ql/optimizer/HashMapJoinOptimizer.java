package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.TablePartition;

/**
 * Added by Brantzhang for hash map join
 * 该类负责处理基于Hash分区的Map Join优化
 */
public class HashMapJoinOptimizer implements Transform {
	  
	  private static final Log LOG = LogFactory.getLog(HashMapJoinOptimizer.class
	      .getName());
	
	  public HashMapJoinOptimizer() {
	  }
	
	  //该方法在优化器Optimizer类中被自动调用
	  public ParseContext transform(ParseContext pctx) throws SemanticException {
	
	    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
	    HashMapjoinOptProcCtx hashMapJoinOptimizeCtx = new HashMapjoinOptProcCtx();
	
	    //以下为需要进行hash map join的模式
	    opRules.put(new RuleRegExp("R1", "MAPJOIN%"), getHashMapjoinProc(pctx));
	    
	    //以下为不需要进行hash map join的模式
	    opRules.put(new RuleRegExp("R2", "RS%.*MAPJOIN"), getHashMapjoinRejectProc(pctx));
	    opRules.put(new RuleRegExp(new String("R3"), "UNION%.*MAPJOIN%"),
	        getHashMapjoinRejectProc(pctx));
	    opRules.put(new RuleRegExp(new String("R4"), "MAPJOIN%.*MAPJOIN%"),
	        getHashMapjoinRejectProc(pctx));
	
	    // 分发器根据最近匹配原则分发各个mapjoin算子
	    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
	        hashMapJoinOptimizeCtx);
	    GraphWalker ogw = new DefaultGraphWalker(disp);
	
	    // Create a list of topop nodes
	    ArrayList<Node> topNodes = new ArrayList<Node>();
	    topNodes.addAll(pctx.getTopOps().values());//获得树中所有根节点
	    ogw.startWalking(topNodes, null);//对树进行遍历
	
	    return pctx;
	  }
	
	  //返回不处理MapJoin时的节点处理器
	  private NodeProcessor getHashMapjoinRejectProc(ParseContext pctx) {
		  return new NodeProcessor() {
	      @Override
	      public Object process(Node nd, Stack<Node> stack,
	          NodeProcessorCtx procCtx, Object... nodeOutputs)
	          throws SemanticException {
	    	  
	        MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
	        HashMapjoinOptProcCtx context = (HashMapjoinOptProcCtx) procCtx;
	        context.listOfRejectedMapjoins.add(mapJoinOp);//将该mapjoin算子加入hashMapJoinOptimizeCtx中的拒绝列表
	        LOG.info("Reject to optimize " + mapJoinOp);
	        
	        return null;
	      }
	    };
	  }
	
	  //返回hash map join处理器
	  private NodeProcessor getHashMapjoinProc(ParseContext pctx) {
		  
	    return new HashMapjoinOptProc(pctx);
	  }
	
	  //返回默认处理器，什么也不做
	  private NodeProcessor getDefaultProc() {
	    return new NodeProcessor() {
	      @Override
	      public Object process(Node nd, Stack<Node> stack,
	          NodeProcessorCtx procCtx, Object... nodeOutputs)
	          throws SemanticException {
	    	  
	        return null;
	      }
	    };
	  }
	  
	  //进行hash map join优化相关信息的收集
	  class HashMapjoinOptProc implements NodeProcessor {
	    
	    protected ParseContext pGraphContext;
	    
	    public HashMapjoinOptProc(ParseContext pGraphContext) {
	      super();
	      this.pGraphContext = pGraphContext;
	    }
	
	    @Override
	    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
	        Object... nodeOutputs) throws SemanticException {
	    	
	      //this.pGraphContext.getConf().setBoolean("hive.mapside.computing", true);
	      //this.pGraphContext.getConf().setBoolean("hive.merge.mapfiles", false);
	      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
	      HashMapjoinOptProcCtx context = (HashMapjoinOptProcCtx) procCtx;
	
	      //如果该map join操作在拒绝列表中，则不做处理
	      if(context.getListOfRejectedMapjoins().contains(mapJoinOp))
	        return null;
	      
	      //获取map join操作的上下文
	      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext().get(mapJoinOp);
	      if(joinCxt == null)
	        return null;
	      
	      List<String> joinAliases = new ArrayList<String>();//用于存放该join操作涉及的所有表
	      String[] srcs = joinCxt.getBaseSrc();
	      String[] left = joinCxt.getLeftAliases();
	      List<String> mapAlias = joinCxt.getMapAliases();//获取所有需要在map端cache住的表名或子查询名
	      String baseBigAlias = null;//需要进行流式读的大数据表
	      for(String s : left) {
	        if(s != null && !joinAliases.contains(s)) {
	          joinAliases.add(s);
	          if(!mapAlias.contains(s)) {
	            baseBigAlias = s;
	          }
	        }
	      }
	      for(String s : srcs) {
	        if(s != null && !joinAliases.contains(s)) {
	          joinAliases.add(s);
	          if(!mapAlias.contains(s)) {
	            baseBigAlias = s;
	          }
	        }
	      }
	      
	      mapJoinDesc mjDecs = mapJoinOp.getConf();
	      
	      //每个数据表对应的Hash分区数
	      LinkedHashMap<String, Integer> aliasToHashPartitionNumber = new LinkedHashMap<String, Integer>();
	      //每个数据表的hash分区到路径的映射
	      LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>> aliasToHashParititionPathNames = new LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>>();
	      
	      // 目前尚不支持 "a join b on a.key = b.key and
	      // a.ds = b.ds"这种查询, 其中ds is a partition column. 
	      
	      Map<String, Operator<? extends Serializable>> topOps = this.pGraphContext.getTopOps();//获得顶层算子
	      Map<TableScanOperator, TablePartition> topToTable = this.pGraphContext.getTopToTable();//获取数据表
	      
	      List<Integer> hashPartitionNumbers = new ArrayList<Integer>();
	      for (int index = 0; index < joinAliases.size(); index++) {//依次处理参与map join的各个表
	        String alias = joinAliases.get(index);//获取表名
	        TableScanOperator tso = (TableScanOperator) topOps.get(alias);
	        TablePartition tbl = topToTable.get(tso);
	        if(!tbl.isPartitioned())
	        	return null;
	        
	        PrunedPartitionList prunedParts = null;
	        try {
	          //进行分区修剪，得到的列表是出hash分区外的最低一级分区的路径
	          prunedParts = PartitionPruner.prune(tbl, pGraphContext.getOpToPartPruner().get(tso), pGraphContext.getConf(), alias);
	            
	        } catch (HiveException e) {
	          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
	          throw new SemanticException(e.getMessage(), e);
	        }
	        
	        LinkedHashMap<Integer, ArrayList<String>> parNumToPaths = new LinkedHashMap<Integer, ArrayList<String>>();//分区号到路径的映射
	        
	        //获取Hash桶到目录路径的对应关系
	        if(tbl.getTTable().getSubPartition() == null){//如果数据表只有一级分区
	          //在这种情况下，一级分区必须是Hash分区
	          Table table = tbl.getTTable();//获得该表的元数据
	          Partition par = table.getPriPartition();
	          if(!par.getParType().equalsIgnoreCase("HASH"))
	        	return null;
	          LOG.info(tbl.getName() + " is partitioned by hash!");
	          
	          //确保进行map join的列就是进行hash分区的列
	          if (!checkHashColumns(par.getParKey().getName(), mjDecs, index))
		        return null;
	          LOG.info(tbl.getName() + " is hash map joined on the hashed column!");
	          
	          //获取该表Hash分区的数目
	          Integer num = new Integer(par.getParSpacesSize());
	          aliasToHashPartitionNumber.put(alias, num);
	          
	          //获取Hash分区的目录名列表，并将其与表名关联
	          Set<String> parspaces = par.getParSpaces().keySet();//获得Hash分区的分区空间，实际上就是所有的分区名
	          //得到各Hash分区的路径
	          int i = 0;
	          for(String parspace: parspaces){
	            ArrayList<String> paths = new ArrayList<String>();
	            String path = tbl.getDataLocation().toString() + "/" + parspace;
	            paths.add(path);
	            parNumToPaths.put(i, paths);
	            i++;
	          }
	          aliasToHashParititionPathNames.put(alias, parNumToPaths);//保存表和路径列表的对应关系
	          
		    }else{//如果数据表有两级分区
	          //在这种情况下，二级分区必须是Hash分区
		      Table table = tbl.getTTable();//获得表的元数据
		      Partition subPar = table.getSubPartition();//二级分区元数据
		      Partition priPar = table.getPriPartition();//一级分区元数据
		          
		      if(!subPar.getParType().equalsIgnoreCase("HASH"))
		        return null;
		      
		      //确保进行map join的列就是进行hash分区的列
	          if (!checkHashColumns(subPar.getParKey().getName(), mjDecs, index))
		            return null;
	          
	          //获取该表Hash分区的数目
	          Integer num = new Integer(subPar.getParSpacesSize());
	          aliasToHashPartitionNumber.put(alias, num);
	          
	          //获取Hash分区的目录名列表，并将其与表名关联
	          	          
	          for(String path: prunedParts.getTargetPartnPaths()){
	        	Set<String> parspaces = subPar.getParSpaces().keySet();//获得Hash分区的分区空间，实际上就是所有的分区名
		        //得到各Hash分区的路径
		        int i = 0;
		        for(String parspace: parspaces){
		          if(path.contains(parspace)){
		        	  ArrayList<String> paths = null;
		        	  if(parNumToPaths.containsKey(i)){
		        		  paths = parNumToPaths.get(i);  
		        	  }else {
		        		  paths = new ArrayList<String>();
		        		  parNumToPaths.put(i, paths);
		        	  }
		        	  paths.add(path);
		        	  break;
		          }
		          i++;
		        }
	          }
	          aliasToHashParititionPathNames.put(alias, parNumToPaths);//保存表和路径列表的对应关系
	        }
	      }
	      
	      // 保证所有表的Hash分区数相同
		  int hashPartitionNoInBigTbl = aliasToHashPartitionNumber.get(baseBigAlias);//获得大表的桶数目
		  Iterator<Integer> iter = aliasToHashPartitionNumber.values().iterator();
		  while(iter.hasNext()) {//依次判断各表的Hash分区数是否等于大表的桶数
		    int nxt = iter.next().intValue();
		    if(nxt != hashPartitionNoInBigTbl){
		    	LOG.info("Not equal in hash partition number!");
		    	return null;
		    }
		      
		  }

	      mapJoinDesc desc = mapJoinOp.getConf();//获得map join的描述文件
		      
		  LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathNameMapping = 
		        new LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>>();
		      
		  for (int j = 0; j < joinAliases.size(); j++) {//依次处理参与mapjoin的各表
		    String alias = joinAliases.get(j);//获得表名
		    
		    if(alias.equals(baseBigAlias))
		      continue;//不处理大表
		    
		    LinkedHashMap<String, ArrayList<String>> mapping = new LinkedHashMap<String, ArrayList<String>>();
		    aliasHashPathNameMapping.put(alias, mapping);//建立小表到<大表桶Hash分区目录，小桶分区目录>的映射关系
		    //LinkedHashMap<Integer, ArrayList<String>> bigTableMap = aliasToHashParititionPathNames.get(baseBigAlias);
		    for(int i = 0; i < hashPartitionNoInBigTbl; i++) {//依次处理每个Hash分区
		      //List<String> bigTblPathsOfI = bigTableMap.get(i);	//获得第i个Hash分区对应的大表目录
		      if(i<10)
		    	  mapping.put("Hash_000"+i, aliasToHashParititionPathNames.get(alias).get(i));
		      else if(i<100)
		    	  mapping.put("Hash_00"+i, aliasToHashParititionPathNames.get(alias).get(i));
		      else if(i<1000)
		    	  mapping.put("Hash_0"+i, aliasToHashParititionPathNames.get(alias).get(i));
		      else 
		    	  return null;
		    }
		  }
		  
		  desc.setAliasHashPathNameMapping(aliasHashPathNameMapping);
		  desc.setBigTableAlias(baseBigAlias);
		  
		  return null;
        }
	   
	    
	    //确保参与map join的列就是进行hash分区的列
	    private boolean checkHashColumns(String hashColumn, mapJoinDesc mjDesc, int index) {
	      List<exprNodeDesc> keys = mjDesc.getKeys().get((byte)index);
	      if (keys == null || hashColumn == null)
	        return false;
	      
	      //获取保存在mapJoinDesc中的所有参与连接的列
	      List<String> joinCols = new ArrayList<String>();
	      List<exprNodeDesc> joinKeys = new ArrayList<exprNodeDesc>();
	      joinKeys.addAll(keys);
	      while (joinKeys.size() > 0) {
	        exprNodeDesc node = joinKeys.remove(0);
	        if (node instanceof exprNodeColumnDesc) {
	          joinCols.addAll(node.getCols());
	        } else if (node instanceof exprNodeGenericFuncDesc) {
	          exprNodeGenericFuncDesc udfNode = ((exprNodeGenericFuncDesc) node);
	          joinKeys.addAll(0, udfNode.getChildExprs());
	        } else if (node instanceof exprNodeFuncDesc) {
	        	exprNodeFuncDesc exNode = (exprNodeFuncDesc) node;
	        	joinKeys.addAll(0, exNode.getChildExprs());
	        }
	        else {
	          return false;
	        }
	      }
	
	      // to see if the join columns from a table is exactly this same as its
	      // bucket columns 
	      if (joinCols.size() != 1) {
	        return false;
	      }
	      for (String col : joinCols) {
	        if (!hashColumn.equals(col))
	          return false;
	      }
	      
	      return true;
	    }
	    
	  }
	  
	  //记录了进行hash map join优化时的上下文，实际上，目前只列出了不应进行hash map join的列表
	  class HashMapjoinOptProcCtx implements NodeProcessorCtx {
	    // 只对mapper中根算子为表扫描算子，紧跟着map join算子的情况进行优化。
		//下表是虽然包括了map join算子，但是不进行优化的子树列表
	    Set<MapJoinOperator> listOfRejectedMapjoins = new HashSet<MapJoinOperator>();
	    
	    public Set<MapJoinOperator> getListOfRejectedMapjoins() {
	    	
	      return listOfRejectedMapjoins;
	    }
	  }
	}
