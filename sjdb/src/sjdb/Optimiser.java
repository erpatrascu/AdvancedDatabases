package sjdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public class Optimiser{
	private Catalogue catalogue;

	private List<Predicate> predicates = new ArrayList<Predicate>(); //used to push the selects down
	private List<Attribute> attributes = new ArrayList<Attribute>(); //used to keep track of what attributes are used
	
	private List<Operator> ilst = new ArrayList<Operator>(); //the initial list of operators
	private List<Operator> olst = new ArrayList<Operator>(); //the optimised list of operators
	
	private Estimator estimator = new Estimator(); 
	private Boolean isFinalProject = false; //used in the case of 'select *' as input
	
	public Optimiser(Catalogue catalogue){
		this.catalogue = catalogue;
	}

	/**
	 * returns the optimised query plan (doesnt re-order joins)
	 * @param cPlan
	 * @return
	 */
	public Operator optimise(Operator cPlan){
		
		parseCanonicalQuery(cPlan);
		transformQueryPlan(cPlan);
		Operator optimisedPlan = olst.get(olst.size()-1);
		
		return optimisedPlan;
	}

	
	/**
	 * Parses the canonical query plan given as input
	 * Stores the predicates, attributes, scans and products in lists to use for optimisation
	 * @param op Canonical query plan
	 */
	
	public void parseCanonicalQuery(Operator op){
		
		//checks the type of operation
		//adds to the specific list and may call the method again
		if(op instanceof Scan){
			ilst.add((Scan)op);
		} else if (op instanceof Project) {
			for (Attribute attribute: (((Project) op).getAttributes())){
				attributes.add(attribute);
			}
			ilst.add((Project)op);
			isFinalProject = true;
			parseCanonicalQuery(((Project) op).getInput());
		} else if (op instanceof Select){
			Predicate predicate = ((Select) op).getPredicate();
		    predicates.add(predicate);
		    
		    if(!predicate.equalsValue()){
		    	attributes.add(predicate.getLeftAttribute());
		    	attributes.add(predicate.getRightAttribute());
		    }
		    
		    ilst.add((Select)op);
		    parseCanonicalQuery(((Select) op).getInput());
		} else if (op instanceof Product){
			ilst.add((Product)op);
			parseCanonicalQuery(((Product) op).getLeft());
			parseCanonicalQuery(((Product) op).getRight());
		} 
	}
	
	/**
	 * optimises the query plan
	 * does the optimisation of each operator one by one
	 * @param cPlan original query plan
	 */
	public void transformQueryPlan(Operator cPlan){
		Collections.reverse(ilst);
		
		Boolean movedSelect = false;		
		
		if(!isFinalProject) {
			getAllAttributes(cPlan, attributes);
		}
		
		for(Operator op : ilst){
			if(op instanceof Scan){
				Scan scan = new Scan((NamedRelation)(((Scan)op).getRelation()));
				
				//this can be a separate method
				//checking which attributes to project
				List<Attribute> attrToProject = new ArrayList<Attribute>();
				List<String> attrNames = getAttributeNames(attributes);
				Boolean projectAllAttrs = true;

				//selecting the list of attributes to project on top of scan
				for(Attribute a : scan.getOutput().getAttributes()){
					if(attrNames.contains(a.getName())){
						attrToProject.add(new Attribute(a));
					} else {
						projectAllAttrs = false;
					}
				}
				
				movedSelect = false;
				
				//going through all the predicates to check if a select can be pushed down on top of this scan
				if (!predicates.isEmpty()){
					ListIterator<Predicate> iter = predicates.listIterator();
					
					Select select = null;
					while(iter.hasNext()){
						Predicate predicate = iter.next();
						
						//check if it's a attr=value predicate
					    if(predicate.equalsValue()){
					    	Attribute lAttr = predicate.getLeftAttribute();
					    	
					    	//checks if the predicate refers to this scan
							if(containsAttribute(op.getOutput(), lAttr)){
								
								//if there is already a select pushed down to the scan, build new selects on top of it
								if(movedSelect){
									select = new Select(select, predicate);
								} else {
									//push down the select to the scan
									select = new Select(scan, predicate);
									movedSelect = true;
								}
								
								iter.remove();
							}
					    }
					}
					
					//to make sure select is not null
					if(movedSelect){
						//if you project all attributes there's no need to add a project operator
						if(projectAllAttrs){
							olst.add(select);
						} else {
							Project project = new Project(select, attrToProject);
							olst.add(project);
						}
						
					}					
				}
				
				if(!movedSelect){
					
					//if you project all attributes there's no need to add a project operator
					if(projectAllAttrs){
						olst.add(scan);
					} else {
						Project project = new Project(scan, attrToProject);
						olst.add(project);
					}
				}	
			} else if (op instanceof Project) {
				continue;
			} else if (op instanceof Select){
				continue;
			} else if (op instanceof Product){
				movedSelect = false;
				Product product = new Product(getOperator(((Product) op).getLeft()), getOperator(((Product) op).getRight()));

				product.accept(estimator);
				estimator.visit(product);
				
				if (!predicates.isEmpty()){
					ListIterator<Predicate> iter = predicates.listIterator();
					
					Select select = null;
					Join join = null;
					while(iter.hasNext()){
						Predicate predicate = iter.next();
						
						//check if it's a attr1=attr2 predicate
					    if(!predicate.equalsValue()){
					    	Attribute lAttr = predicate.getLeftAttribute();
							Attribute rAttr = predicate.getRightAttribute();
							
							Relation prod = product.getOutput();
							if(containsAttribute(prod, lAttr) && containsAttribute(prod, rAttr)){

								if(movedSelect){
									if(select == null){
										select = new Select(join, predicate);
									} else {
										select = new Select(select, predicate);
									} 
								} else {

									//combine product and select into a join
									join = new Join(product.getLeft(), product.getRight(), predicate);
									join.accept(estimator);
									estimator.visit(join);
									
									movedSelect = true;
								}
								
								//remove the join attributes from the list of remaining attributes needed in the plan	
								attributes.remove(lAttr);
								attributes.remove(rAttr);

								iter.remove();
							}	
						}
					}
					
					if(movedSelect){
						//list of names for easier comparison
						List<Attribute> attrToProject = new ArrayList<Attribute>();
						List<String> attrNames = getAttributeNames(attributes);
						
						int countLAttr = Collections.frequency(attrNames, join.getPredicate().getLeftAttribute().getName());
						int countRAttr = Collections.frequency(attrNames, join.getPredicate().getRightAttribute().getName());
						
						
						//if the 2 attributes in the join predicate are used somewhere else
						//then don't project anything; else project the rest of the attributes
						if(countLAttr>0 && countRAttr>0){
							if(select == null){
								olst.add(join);
							} else {
								olst.add(select);
							}
							
						} else {
							//selecting the list of attributes to project on top of join
							for(Attribute a : join.getOutput().getAttributes()){
								if(attrNames.contains(a.getName())){
									attrToProject.add(new Attribute(a));
								} 
							} 
							
							Project project = null;
							if(select == null){
								project = new Project(join, attrToProject);
							} else {
								project = new Project(select, attrToProject);
							}
							
							olst.add(project);
						}
					}
				}
				
				//if there was no select operation pushed down on top of product, then just add the product
				if(!movedSelect){
					olst.add(product);
				}
			}
		}
	}

	/**
	 * used as helper function, gets all scans present in an operator
	 * @param op operator
	 * @param allScans a set of scans
	 */
	private void getAllScans(Operator op, Set<String> allScans){
		
		if(op instanceof Scan){
			allScans.add(((Scan) op).getRelation().toString());
		} else if(op instanceof UnaryOperator){
			getAllScans(((UnaryOperator) op).getInput(), allScans);
		} else {
			getAllScans(op.getInputs().get(0), allScans);
			getAllScans(op.getInputs().get(1), allScans);
		}
	}
	
	/**
	 * to get all attributes of an operator
	 * used in the case when all the attributes are projected
	 * @param op
	 * @param allAttributes
	 */
	private void getAllAttributes(Operator op, List<Attribute> allAttributes){
		if(op instanceof Scan){
			for (Attribute attribute:((Scan) op).getRelation().getAttributes()){
				allAttributes.add(attribute);
			}
		} else if(op instanceof UnaryOperator){
			getAllAttributes(((UnaryOperator) op).getInput(), allAttributes);
		} else {
			getAllAttributes(op.getInputs().get(0), allAttributes);
			getAllAttributes(op.getInputs().get(1), allAttributes);
		}
	}
	
	
	/**
	 * gets the operator that corresponds to the changed versions of the inputs to a product or join
	 * used to create the final operator
	 * @param op operator
	 * @return
	 */
	private Operator getOperator(Operator op){
		Set<String> allScans = new HashSet<String>();
		getAllScans(op, allScans);
		
		for(Operator operator:olst){
			Set<String> allScansOpt = new HashSet<String>();
			getAllScans(operator, allScansOpt);
			if(allScans.equals(allScansOpt)){
				return operator;
			}
		}
		return null;
		
	}
	
	
	 /**
	  * checks if a relation contains a certain attribute
	  * @param rel relation
	  * @param attr attribute
	  * @return
	  */
	private Boolean containsAttribute(Relation rel, Attribute attr){
		for (Attribute a : rel.getAttributes()){
			if(a.getName().equals(attr.getName())){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * creates a list of attribute names from a list of attributes
	 * @param attrs
	 * @return
	 */
	private List<String> getAttributeNames(List<Attribute> attrs){
		List<String> attrNames = new ArrayList<String>();
		for (Attribute a:attrs){
			attrNames.add(a.getName());
		}
		
		return attrNames;
	}
	
}
