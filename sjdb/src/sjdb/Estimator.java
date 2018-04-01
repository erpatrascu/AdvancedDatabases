package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}


	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		
		//creating the output relation with the correspondent tuple count
		Relation output = new Relation(input.getTupleCount());
		
		List<Attribute> attributes = op.getAttributes();
		List<String> attributesNames = new ArrayList<String>();
		for(Attribute attr: attributes){
			attributesNames.add(attr.getName());
		}
		
		//adding to the output relation only the attributes projected
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute attribute = new Attribute(iter.next());
			if(attributesNames.contains(attribute.getName())) {
				output.addAttribute(attribute);
			}
		}
		
		op.setOutput(output);
	}
	
	
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Predicate predicate = op.getPredicate();
		Attribute leftAttribute = predicate.getLeftAttribute();
		
		if(predicate.equalsValue()){
			Attribute lAttr = input.getAttribute(leftAttribute);
			
			//creating the output relation with the correspondent tuple count
			Relation output = new Relation(input.getTupleCount()/lAttr.getValueCount());
			
			//add the attributes to the output relation, with the correct value counts
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = new Attribute(iter.next());
				if(attr.equals(lAttr)){
					output.addAttribute(new Attribute(attr.getName(), 1));
				} else {
					output.addAttribute(attr);
				}
			}
			
			op.setOutput(output);
		
		} else {
			Attribute rightAttribute = predicate.getRightAttribute();
			
			//getting the correspondent attributes from the relation
			Attribute lAttr = input.getAttribute(leftAttribute);
			Attribute rAttr = input.getAttribute(rightAttribute);
			
			//creating the output relation with the correspondent tuple count
			Relation output = new Relation(input.getTupleCount()/Math.max(lAttr.getValueCount(),rAttr.getValueCount()));
			
			//add the attributes to the output relation, with the correct value counts
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attr = new Attribute(iter.next());
				if((attr.equals(lAttr)) || (attr.equals(rAttr))){
					output.addAttribute(new Attribute(attr.getName(), Math.min(lAttr.getValueCount(), rAttr.getValueCount())));
				} else {
					output.addAttribute(attr);
				}	
			}
			
			op.setOutput(output);	
		}	
	}
	
	
	public void visit(Product op) {
		
		//getting the left and right relations
		Relation inputLeft = op.getLeft().getOutput();
		Relation inputRight = op.getRight().getOutput();
		
		//creating an output relation with the tuple cost being the product of the 2 costs
		Relation output = new Relation(inputLeft.getTupleCount() * inputRight.getTupleCount());
		
		//the new relations consists of the attributes contained in the 2 initial relations
		Iterator<Attribute> iterLeft = inputLeft.getAttributes().iterator();
		while (iterLeft.hasNext()) {
			output.addAttribute(new Attribute(iterLeft.next()));
		}
		
		Iterator<Attribute> iterRight = inputRight.getAttributes().iterator();
		while (iterRight.hasNext()) {
			output.addAttribute(new Attribute(iterRight.next()));
		}
		
		op.setOutput(output);
	}
	
	
	public void visit(Join op) {
		
		//getting the left and right relations, and predicate attributes
		Relation inputLeft = op.getLeft().getOutput();
		Relation inputRight = op.getRight().getOutput();
		Predicate predicate = op.getPredicate();
		Attribute leftAttribute = predicate.getLeftAttribute();
		Attribute rightAttribute = predicate.getRightAttribute();
		
		//getting the input relations attributes based on the predicate attributes
		Attribute lAttr = inputLeft.getAttribute(leftAttribute);
		Attribute rAttr = inputRight.getAttribute(rightAttribute);
		
		//creating an output relation with the correspondent tuple count
		Relation output = new Relation((inputLeft.getTupleCount() * inputRight.getTupleCount())/Math.max(lAttr.getValueCount(), rAttr.getValueCount()));
		
		//the new relations consists of the attributes contained in the 2 initial relations
		//with some changes to the value counts 
		Iterator<Attribute> iterLeft = inputLeft.getAttributes().iterator();
		while (iterLeft.hasNext()) {
			Attribute attr = new Attribute(iterLeft.next());
			if(attr.equals(lAttr)){
				output.addAttribute(new Attribute(attr.getName(), Math.min(lAttr.getValueCount(), rAttr.getValueCount())));
			} else {
				output.addAttribute(attr);
			}
		}
		
		Iterator<Attribute> iterRight = inputRight.getAttributes().iterator();
		while (iterRight.hasNext()) {
			Attribute attr = new Attribute(iterRight.next());
			if(attr.equals(rAttr)){
				output.addAttribute(new Attribute(attr.getName(), Math.min(lAttr.getValueCount(), rAttr.getValueCount())));
			} else {
				output.addAttribute(attr);
			}
		}
		
		op.setOutput(output);
	}
}
