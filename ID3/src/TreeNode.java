import java.util.ArrayList;
import java.util.List;

public class TreeNode 
{
	public int attribute;
	public String feature;
	public String leafValue;
	public List<Integer> childNode;
	
	public TreeNode()
	{
		this.attribute = 0;
		this.feature = "";
		this.leafValue = "";
		this.childNode = new ArrayList<Integer>();
	}
	
	public TreeNode(int atb, String ftr)
	{
		this.attribute = atb;
		this.feature = ftr;
		this.leafValue = "";
		this.childNode = new ArrayList<Integer>();
	}
	
}
