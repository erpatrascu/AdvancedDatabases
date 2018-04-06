package sjdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;




public class Optimiser{
	private Catalogue catalogue;
	
	private List<Scan> scans = new ArrayList<Scan>();
	private List<Product> products = new ArrayList<Product>();
	private List<Predicate> predicates = new ArrayList<Predicate>();
	private List<Attribute> attributes = new ArrayList<Attribute>();
	
	private List<Operator> ilst = new ArrayList<Operator>();
	
	private Estimator estimator = new Estimator();
	
	public Optimiser(Catalogue catalogue){
		this.catalogue = catalogue;
	}

	
	
	public Operator optimise(Operator cPlan){
		
		parseCanonicalQuery(cPlan);
		List<Operator> olst = transformQueryPlan();
		System.out.println(olst);
		
		
		return null;
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
			System.out.println("SCAN");
		} else if (op instanceof Project) {
			for (Attribute attribute: (((Project) op).getAttributes())){
				attributes.add(attribute);
			}
			ilst.add((Project)op);
			System.out.println("PROJECT");
			parseCanonicalQuery(((Project) op).getInput());
		} else if (op instanceof Select){
			Predicate predicate = ((Select) op).getPredicate();
		    predicates.add(predicate);
		    
		    if(!predicate.equalsValue()){
		    	attributes.add(predicate.getLeftAttribute());
		    	attributes.add(predicate.getRightAttribute());
		    }
		    
		    ilst.add((Select)op);
		    System.out.println("SELECT");
		    parseCanonicalQuery(((Select) op).getInput());
		} else if (op instanceof Product){
			//products.add(new Product(((Product) op).getLeft(), ((Product) op).getRight()));
			ilst.add((Product)op);
			System.out.println("PRODUCT");
			//estimator.visit(products.get(products.size()-1));
			parseCanonicalQuery(((Product) op).getLeft());
			parseCanonicalQuery(((Product) op).getRight());
		} else if (op instanceof Join){
			
		} 
	}
	
	
	public List<Operator> transformQueryPlan(){
		Collections.reverse(ilst);
		List<Operator> olst = new ArrayList<Operator>();
		Boolean movedSelect = false;
		
		System.out.println(ilst.size());
		
		
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
							//olst.add(scan);
							olst.add(select);
							System.out.println(select);
						} else {
							Project project = new Project(select, attrToProject);
							//olst.add(scan);
							//olst.add(select);
							olst.add(project);
							System.out.println(project);
						}
						
					}					
				}
				
				if(!movedSelect){
					
					//if you project all attributes there's no need to add a project operator
					if(projectAllAttrs){
						olst.add(scan);
						System.out.println(scan);
					} else {
						Project project = new Project(scan, attrToProject);
						olst.add(project);
						System.out.println(project);
					}
				}
				
				
				
			} else if (op instanceof Project) {
				//Project project = new Project(((Project) op).getInput(), ((Project) op).getAttributes());
				//System.out.println("Project " + pattributes.toString());
				//olst.add(project);
				//System.out.println(project);
				continue;
			} else if (op instanceof Select){
				continue;
			} else if (op instanceof Product){
				movedSelect = false;
				Product product = new Product(((Product) op).getLeft(), ((Product) op).getRight());
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
								System.out.println(join);
							} else {
								olst.add(select);
								System.out.println(select);
							}
							
						} else {
							//selecting the list of attributes to project on top of join
							for(Attribute a : join.getOutput().getAttributes()){
								if(attrNames.contains(a.getName())){
									attrToProject.add(new Attribute(a));
								} 
							} 
							
							Project project = null;
							//join.setOutput(null);
							if(select == null){
								project = new Project(join, attrToProject);
							} else {
								project = new Project(select, attrToProject);
							}
							
							olst.add(project);
							System.out.println(project);
						}
					}
					
					
				}
				
				//if there was no select operation pushed down on top of product, then just add the product
				if(!movedSelect){
					//need to check if this works well
					//product.setOutput(null);
					olst.add(product);
					System.out.println(product);
				}
			}
		}
		
		return olst;
		
	}

	
	private Boolean containsAttribute(Relation rel, Attribute attr){
		for (Attribute a : rel.getAttributes()){
			if(a.getName().equals(attr.getName())){
				return true;
			}
		}
		return false;
	}
	
	private List<String> getAttributeNames(List<Attribute> attrs){
		List<String> attrNames = new ArrayList<String>();
		for (Attribute a:attrs){
			attrNames.add(a.getName());
		}
		
		return attrNames;
	}
	
}
