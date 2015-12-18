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
 * ���ฺ�������Hash������Map Join�Ż�
 */
public class HashMapJoinOptimizer implements Transform {
	  
	  private static final Log LOG = LogFactory.getLog(HashMapJoinOptimizer.class
	      .getName());
	
	  public HashMapJoinOptimizer() {
	  }
	
	  //�÷������Ż���Optimizer���б��Զ�����
	  public ParseContext transform(ParseContext pctx) throws SemanticException {
	
	    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
	    HashMapjoinOptProcCtx hashMapJoinOptimizeCtx = new HashMapjoinOptProcCtx();
	
	    //����Ϊ��Ҫ����hash map join��ģʽ
	    opRules.put(new RuleRegExp("R1", "MAPJOIN%"), getHashMapjoinProc(pctx));
	    
	    //����Ϊ����Ҫ����hash map join��ģʽ
	    opRules.put(new RuleRegExp("R2", "RS%.*MAPJOIN"), getHashMapjoinRejectProc(pctx));
	    opRules.put(new RuleRegExp(new String("R3"), "UNION%.*MAPJOIN%"),
	        getHashMapjoinRejectProc(pctx));
	    opRules.put(new RuleRegExp(new String("R4"), "MAPJOIN%.*MAPJOIN%"),
	        getHashMapjoinRejectProc(pctx));
	
	    // �ַ����������ƥ��ԭ��ַ�����mapjoin����
	    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
	        hashMapJoinOptimizeCtx);
	    GraphWalker ogw = new DefaultGraphWalker(disp);
	
	    // Create a list of topop nodes
	    ArrayList<Node> topNodes = new ArrayList<Node>();
	    topNodes.addAll(pctx.getTopOps().values());//����������и��ڵ�
	    ogw.startWalking(topNodes, null);//�������б���
	
	    return pctx;
	  }
	
	  //���ز�����MapJoinʱ�Ľڵ㴦����
	  private NodeProcessor getHashMapjoinRejectProc(ParseContext pctx) {
		  return new NodeProcessor() {
	      @Override
	      public Object process(Node nd, Stack<Node> stack,
	          NodeProcessorCtx procCtx, Object... nodeOutputs)
	          throws SemanticException {
	    	  
	        MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
	        HashMapjoinOptProcCtx context = (HashMapjoinOptProcCtx) procCtx;
	        context.listOfRejectedMapjoins.add(mapJoinOp);//����mapjoin���Ӽ���hashMapJoinOptimizeCtx�еľܾ��б�
	        LOG.info("Reject to optimize " + mapJoinOp);
	        
	        return null;
	      }
	    };
	  }
	
	  //����hash map join������
	  private NodeProcessor getHashMapjoinProc(ParseContext pctx) {
		  
	    return new HashMapjoinOptProc(pctx);
	  }
	
	  //����Ĭ�ϴ�������ʲôҲ����
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
	  
	  //����hash map join�Ż������Ϣ���ռ�
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
	
	      //�����map join�����ھܾ��б��У���������
	      if(context.getListOfRejectedMapjoins().contains(mapJoinOp))
	        return null;
	      
	      //��ȡmap join������������
	      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext().get(mapJoinOp);
	      if(joinCxt == null)
	        return null;
	      
	      List<String> joinAliases = new ArrayList<String>();//���ڴ�Ÿ�join�����漰�����б�
	      String[] srcs = joinCxt.getBaseSrc();
	      String[] left = joinCxt.getLeftAliases();
	      List<String> mapAlias = joinCxt.getMapAliases();//��ȡ������Ҫ��map��cacheס�ı������Ӳ�ѯ��
	      String baseBigAlias = null;//��Ҫ������ʽ���Ĵ����ݱ�
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
	      
	      //ÿ�����ݱ��Ӧ��Hash������
	      LinkedHashMap<String, Integer> aliasToHashPartitionNumber = new LinkedHashMap<String, Integer>();
	      //ÿ�����ݱ��hash������·����ӳ��
	      LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>> aliasToHashParititionPathNames = new LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>>();
	      
	      // Ŀǰ�в�֧�� "a join b on a.key = b.key and
	      // a.ds = b.ds"���ֲ�ѯ, ����ds is a partition column. 
	      
	      Map<String, Operator<? extends Serializable>> topOps = this.pGraphContext.getTopOps();//��ö�������
	      Map<TableScanOperator, TablePartition> topToTable = this.pGraphContext.getTopToTable();//��ȡ���ݱ�
	      
	      List<Integer> hashPartitionNumbers = new ArrayList<Integer>();
	      for (int index = 0; index < joinAliases.size(); index++) {//���δ������map join�ĸ�����
	        String alias = joinAliases.get(index);//��ȡ����
	        TableScanOperator tso = (TableScanOperator) topOps.get(alias);
	        TablePartition tbl = topToTable.get(tso);
	        if(!tbl.isPartitioned())
	        	return null;
	        
	        PrunedPartitionList prunedParts = null;
	        try {
	          //���з����޼����õ����б��ǳ�hash����������һ��������·��
	          prunedParts = PartitionPruner.prune(tbl, pGraphContext.getOpToPartPruner().get(tso), pGraphContext.getConf(), alias);
	            
	        } catch (HiveException e) {
	          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
	          throw new SemanticException(e.getMessage(), e);
	        }
	        
	        LinkedHashMap<Integer, ArrayList<String>> parNumToPaths = new LinkedHashMap<Integer, ArrayList<String>>();//�����ŵ�·����ӳ��
	        
	        //��ȡHashͰ��Ŀ¼·���Ķ�Ӧ��ϵ
	        if(tbl.getTTable().getSubPartition() == null){//������ݱ�ֻ��һ������
	          //����������£�һ������������Hash����
	          Table table = tbl.getTTable();//��øñ��Ԫ����
	          Partition par = table.getPriPartition();
	          if(!par.getParType().equalsIgnoreCase("HASH"))
	        	return null;
	          LOG.info(tbl.getName() + " is partitioned by hash!");
	          
	          //ȷ������map join���о��ǽ���hash��������
	          if (!checkHashColumns(par.getParKey().getName(), mjDecs, index))
		        return null;
	          LOG.info(tbl.getName() + " is hash map joined on the hashed column!");
	          
	          //��ȡ�ñ�Hash��������Ŀ
	          Integer num = new Integer(par.getParSpacesSize());
	          aliasToHashPartitionNumber.put(alias, num);
	          
	          //��ȡHash������Ŀ¼���б����������������
	          Set<String> parspaces = par.getParSpaces().keySet();//���Hash�����ķ����ռ䣬ʵ���Ͼ������еķ�����
	          //�õ���Hash������·��
	          int i = 0;
	          for(String parspace: parspaces){
	            ArrayList<String> paths = new ArrayList<String>();
	            String path = tbl.getDataLocation().toString() + "/" + parspace;
	            paths.add(path);
	            parNumToPaths.put(i, paths);
	            i++;
	          }
	          aliasToHashParititionPathNames.put(alias, parNumToPaths);//������·���б�Ķ�Ӧ��ϵ
	          
		    }else{//������ݱ�����������
	          //����������£���������������Hash����
		      Table table = tbl.getTTable();//��ñ��Ԫ����
		      Partition subPar = table.getSubPartition();//��������Ԫ����
		      Partition priPar = table.getPriPartition();//һ������Ԫ����
		          
		      if(!subPar.getParType().equalsIgnoreCase("HASH"))
		        return null;
		      
		      //ȷ������map join���о��ǽ���hash��������
	          if (!checkHashColumns(subPar.getParKey().getName(), mjDecs, index))
		            return null;
	          
	          //��ȡ�ñ�Hash��������Ŀ
	          Integer num = new Integer(subPar.getParSpacesSize());
	          aliasToHashPartitionNumber.put(alias, num);
	          
	          //��ȡHash������Ŀ¼���б����������������
	          	          
	          for(String path: prunedParts.getTargetPartnPaths()){
	        	Set<String> parspaces = subPar.getParSpaces().keySet();//���Hash�����ķ����ռ䣬ʵ���Ͼ������еķ�����
		        //�õ���Hash������·��
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
	          aliasToHashParititionPathNames.put(alias, parNumToPaths);//������·���б�Ķ�Ӧ��ϵ
	        }
	      }
	      
	      // ��֤���б��Hash��������ͬ
		  int hashPartitionNoInBigTbl = aliasToHashPartitionNumber.get(baseBigAlias);//��ô���Ͱ��Ŀ
		  Iterator<Integer> iter = aliasToHashPartitionNumber.values().iterator();
		  while(iter.hasNext()) {//�����жϸ����Hash�������Ƿ���ڴ���Ͱ��
		    int nxt = iter.next().intValue();
		    if(nxt != hashPartitionNoInBigTbl){
		    	LOG.info("Not equal in hash partition number!");
		    	return null;
		    }
		      
		  }

	      mapJoinDesc desc = mapJoinOp.getConf();//���map join�������ļ�
		      
		  LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathNameMapping = 
		        new LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>>();
		      
		  for (int j = 0; j < joinAliases.size(); j++) {//���δ������mapjoin�ĸ���
		    String alias = joinAliases.get(j);//��ñ���
		    
		    if(alias.equals(baseBigAlias))
		      continue;//��������
		    
		    LinkedHashMap<String, ArrayList<String>> mapping = new LinkedHashMap<String, ArrayList<String>>();
		    aliasHashPathNameMapping.put(alias, mapping);//����С��<���ͰHash����Ŀ¼��СͰ����Ŀ¼>��ӳ���ϵ
		    //LinkedHashMap<Integer, ArrayList<String>> bigTableMap = aliasToHashParititionPathNames.get(baseBigAlias);
		    for(int i = 0; i < hashPartitionNoInBigTbl; i++) {//���δ���ÿ��Hash����
		      //List<String> bigTblPathsOfI = bigTableMap.get(i);	//��õ�i��Hash������Ӧ�Ĵ��Ŀ¼
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
	   
	    
	    //ȷ������map join���о��ǽ���hash��������
	    private boolean checkHashColumns(String hashColumn, mapJoinDesc mjDesc, int index) {
	      List<exprNodeDesc> keys = mjDesc.getKeys().get((byte)index);
	      if (keys == null || hashColumn == null)
	        return false;
	      
	      //��ȡ������mapJoinDesc�е����в������ӵ���
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
	  
	  //��¼�˽���hash map join�Ż�ʱ�������ģ�ʵ���ϣ�Ŀǰֻ�г��˲�Ӧ����hash map join���б�
	  class HashMapjoinOptProcCtx implements NodeProcessorCtx {
	    // ֻ��mapper�и�����Ϊ��ɨ�����ӣ�������map join���ӵ���������Ż���
		//�±�����Ȼ������map join���ӣ����ǲ������Ż��������б�
	    Set<MapJoinOperator> listOfRejectedMapjoins = new HashSet<MapJoinOperator>();
	    
	    public Set<MapJoinOperator> getListOfRejectedMapjoins() {
	    	
	      return listOfRejectedMapjoins;
	    }
	  }
	}
