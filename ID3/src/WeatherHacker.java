import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WeatherHacker 
{
	public static HashMap<String, Integer> feature_count = new HashMap<>();//Đếm trạng thái
	public static HashMap<Integer, String> intermediate = new HashMap<>();//Bộ đệm
	public static HashMap<String, Double> feature_entropy = new HashMap<>();//Lưu giá tính entroty của trạng thái node
	
	public static int data_size = 0; 
	public static int current_attrib = 0;//Tạm thời là độ sâu hiện tại nó đang xét
	public static String current_feature = "";//Trạng thái hiện tại của node
	public static String result = "";//Kết quả Yes/No
	
	public static HashMap <Integer, TreeNode> tree = new HashMap<Integer, TreeNode>();
	
	
	public static void clear()
	{
		feature_count.clear();
		intermediate.clear();
		feature_entropy.clear();
	}
	
	public static int calculateMinimumEntropy()//Hàm tính giá trị entropy nhỏ nhất
	{
		double minimum = Double.POSITIVE_INFINITY;
		int return_val = 0;
		int izz=0;
		//Đếm số lần xuất hiện của trạng thái vào lưu vào feature_count(key,value) với key tên trạng thái và value là số lần xuất hiện  hot-5-high;5 [hot,high]
		
		/*
		 * 
		 */
		 
		for (Entry<String, Integer> entry : feature_count.entrySet())
		{
//			System.out.print("Entry:"+entry+"\n");//entry dạng key-value:(key-thuộc tính+trạng thái+giá trị(yes/no) ,value:số lần xuất hiện))
			String[] key = entry.getKey().split("-");
			System.out.print("Key[0]:"+key[0]+"\t"+"Key[1]:"+key[1]+"\t"+"Key[2]:"+key[2]+"\t"+"\t"+"Entry:"+entry.getValue()+ "\n");
			int sub_key = Integer.parseInt(key[0]);
		
			if(intermediate.containsKey(sub_key))
			{
				intermediate.put(sub_key, intermediate.get(sub_key) + ";" +  key[1]+ "-" + entry.getValue());
			}
			else intermediate.put(sub_key, key[1] + "-" + entry.getValue());
		}
		//intermediate
		//key=0 ,value=overcast-4;overcast-3;sunny-3;sunny-2;rainy-2;rainy-3
		//key=1 hot-2;cool-1;
		//key=2 normal-1;high-3
		//key=3 weak-1;strong-3;weak-3;strong-2
		//entropy(3,4)=entropy(4/3)
		
		for (Entry<Integer, String> entry : intermediate.entrySet())
		{
			//Xét từ entry với key=0 và value=overcast-4;overcast-3;sunny-3;sunny-2;rainy-2;rainy-3
			HashMap<String, String> attrib = new HashMap<>();
			String attrib_list = entry.getValue();//attrib_list=overcast-4;overcast-3;sunny-3;sunny-2;rainy-2;rainy-3
			String [] split = attrib_list.split(";");//split=[overcast-4,overcast-3,sunny-3,sunny-2,rainy-2,rainy-3]
			for(int i = 0; i < split.length; i++)
			{
				String[] k_v = split[i].split("-");
				String k = k_v[0];//overcast
				String v = k_v[1];//4
				if(attrib.containsKey(k))
				{
					attrib.put(k, attrib.get(k) + "-" + v);//attrib(key:overcast,value:4-3) key=1;value=a  get(1)=a
				}
				else attrib.put(k, v);//atrib(key:overcast,value:4)
			}
			
//			overcast,4-3
//			hot,2-3
//			weak,4-5
			
			
			double attrib_entropy = 0;
			for(Entry<String, String> ent : attrib.entrySet())
			{
				//ent=[overcast,4-3]
				double local_entropy = 0.0;
				int local_size = 0;
				String[] ent_v = ent.getValue().split("-");//ent-v chứa giá trị [4,3]
				for(int i = 0; i < ent_v.length; i++)
				{
					local_size += Integer.parseInt(ent_v[i]);//Đếm số lần xuất hiện của cái thuộc tính đó (cả yes/no)
				}
				//sau bước nay local_size=7
				
				for(int i = 0; i < ent_v.length; i++)
				{
					double ratio =1.0 * Integer.parseInt(ent_v[i]) / local_size;//Tại đây ent_v[i]=4,localsize=7==>ratio= 4/7
					local_entropy += -1 * ratio * Math.log(ratio);//Công thức tính entropy :-4/7log2(4/7)
					
					/*Tại bước lặp  i=0
					 *  ratio=4/7
					 * local_entropy=-4/7log2(4/7)
					 */
					
					/*Tại bước lặp tiếp theo tại i=1
					 *  ratio=3/7
					 * local_entropy=-4/7log2(4/7)-3/7log2(3/7)
					 */
				}	
				//* local_entropy=-4/7log2(4/7)-3/7log2(3/7)
				
				
				//entry.getkey()=0
				//ent.getkey()=overcast
				//local_entropy=-4/7log2(4/7)-3/7log2(3/7)
				
				//feature_entropy có dạng key-value với key=(0-overcast) và value =(-4/7log2(4/7)-3/7log2(3/7))
				feature_entropy.put(entry.getKey() + "-" + ent.getKey(), local_entropy);
				
				//attrib_entropy =5/14*0.971+4/14*0+5/14*0.971
				attrib_entropy += 1.0 * local_size/data_size * local_entropy;	
			}
			if(attrib_entropy < minimum)
			{
				minimum = attrib_entropy;
				return_val = entry.getKey();//entry.getkey() trả về 0,1,2,3
			}
		}
		
		return return_val;//trả về key có entropy nhỏ nhất(0)
	}
	
	
	public static void setChildNode(int curr_node)
	{
		//feature_entropy có dạng key-value với key=(0-overcast) và value(entropy) =(-4/7log2(4/7)-3/7log2(3/7))
		for (Entry<String, Double> entry : feature_entropy.entrySet())
		{
			int attb = Integer.parseInt(entry.getKey().split("-")[0]);//attb(thuộc tính) =0
			String ftr = entry.getKey().split("-")[1];//ftr(trạng thái)=overcast
			{
				if(attb == current_attrib)//kiểm tra xem có đang đứng ở thuộc tính hiện tại hay k
				{
					TreeNode node = new TreeNode(attb, ftr);
					if(entry.getValue() == 0.0)//Nếu entropy=0
					{
						//ent dạng key-value:(key-thuộc tính+trạng thái+giá trị(yes/no) ,value:số lần xuất hiện))
						for (Entry<String, Integer> ent : feature_count.entrySet())
						{
							String[] split = ent.getKey().split("-");
							String check = split[0] + "-" + split[1];//check=0-overcast
							if(entry.getKey().equals(check))
							{
								node.leafValue = split[2];//node=yes
							}
						}
					}
					//tree có cấu tạo key:kích thước cây value:node 
					tree.put(tree.size(), node);
				//Đi ngược lại để xét các thuộc tính tiếp theo
					tree.get(curr_node).childNode.add(tree.size() - 1 );
				}
			}
		}
	}
	
//
	public static void getResult(List<String> atb, TreeNode node)
	{
		if(node.leafValue.length() != 0)
		{
			result = node.leafValue;
			return;
		}
		else
		{
			for(int i = 0; i < node.childNode.size(); i++)
			{
				TreeNode nd = tree.get(node.childNode.get(i));
				if(atb.get(nd.attribute).equals(nd.feature))
				{
					getResult(atb, nd);
				}
			}
		}

		return;
	}
	
	public static void printBranch(List<String> branch, TreeNode node)
	{
		if(node.leafValue.length() != 0)
		{
			branch.add(node.feature);
			System.out.println(branch + "->" + node.leafValue);
			branch.remove(branch.size() - 1);
			return;
		}
		else
		{
			for(int i = 0; i < node.childNode.size(); i++)
			{
				branch.add(node.feature);
				printBranch(branch, tree.get(node.childNode.get(i)));
				branch.remove(branch.size() - 1);
			}
		}
	}
	
	
	
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		JobClient my_client = new JobClient();
		JobConf job_conf = new JobConf(WeatherHacker.class);

		job_conf.setJobName("Word Count1");

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		job_conf.setMapperClass(WeatherHackerMapper.class);
		job_conf.setReducerClass(WeatherHackerReducer.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		
		
		long now = System.currentTimeMillis();
		FileInputFormat.setInputPaths(job_conf, new Path("C:\\hadoop\\test\\project\\input\\input.txt"));
		
		TreeNode root = new TreeNode();//Tạo node
		tree.put(0, root);//Đưa vào node đầu tiên của cây
		
		int curr_node = 0;//vị trí node hiện tại đang duyệt
		while(curr_node < tree.size())//lặp tới khi đi hết cây
		{
			if(tree.get(curr_node).leafValue.length() == 0)//Nếu node hiện tại không có node con
			{
				current_attrib =  tree.get(curr_node).attribute;
				current_feature = tree.get(curr_node).feature;
				
				
				FileOutputFormat.setOutputPath(job_conf, new Path("C:\\hadoop\\test\\project\\output\\outputXuanCanh" + String.valueOf(now)));
				my_client.setConf(job_conf);
				try 
				{
					JobClient.runJob(job_conf);
				
					
					String currentpath = "C:\\hadoop\\test\\project\\output\\outputXuanCanh" + String.valueOf(now) + "\\part-00000";
					FileSystem fs1 = FileSystem.get(new Configuration());
					BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(new Path(currentpath))));
					String l = br1.readLine();
					while (l != null)
					{
						String [] split = l.split("\t");
						if(split[0].equals("size"))
						{
							data_size = Integer.parseInt(split[1]);
						}
						else
						{
							feature_count.put(split[0], Integer.parseInt(split[1]));
						}
						l = br1.readLine();
					}
					current_attrib = calculateMinimumEntropy();
					setChildNode(curr_node);
				} 
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
			clear();
			now = System.currentTimeMillis();
			curr_node++;
		}
		System.out.println("Model Complete");
		
		String con = "";
		List<String> atb_list = new ArrayList<String>();
		
		printBranch(atb_list, tree.get(0));
		
		do
		{
			atb_list.clear();
			Scanner in = new Scanner(System.in);
			System.out.println("\nTest model");
			System.out.println("outlook attribute(sunny / overcast / rainy): ");
			atb_list.add(in.nextLine());
			System.out.println("temperature attribute(hot / mild / cold): ");
			atb_list.add(in.nextLine());
			System.out.println("humidity attribute(high / normal): ");
			atb_list.add(in.nextLine());
			System.out.println("wind attribute(strong / weak): ");
			atb_list.add(in.nextLine());
			
			getResult(atb_list, tree.get(0));
			
			System.out.println("Decision: " + result);
			System.out.println("Try again? (y/n)");
			con = in.nextLine();
		}
		while (con.equals("y"));
		
		
		System.out.println("Finish!");
	}
}
