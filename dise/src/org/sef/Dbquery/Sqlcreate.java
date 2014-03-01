package org.sef.Dbquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.sef.DbObjects.school;



public class Sqlcreate {
String SELECT="select * from school";
public List<school> selectfromschool() throws ClassNotFoundException, SQLException
{
	List<school> list=new ArrayList<school>();
	Class.forName("com.mysql.jdbc.Driver");
	Connection conn = null;
	conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dise","root", "monibuvyct");
	conn.close();
	try
	{
	String sql=SELECT;
	PreparedStatement pst=conn.prepareStatement(sql);
	
	ResultSet rs=pst.executeQuery();
	while(rs.next()){
		school school = new school();
		school.setAddress(rs.getString("address"));
		school.setLat(rs.getDouble("lat"));
		school.setLng(rs.getDouble("lng"));
		school.setNelat(rs.getDouble("nelat"));
		school.setNelng(rs.getDouble("nelng"));
		school.setSelat(rs.getDouble("selat"));
		school.setSelng(rs.getDouble("selng"));
		school.setSchoolid(rs.getInt("schoolid"));
		school.setSchoolname(rs.getString("schoolname"));
		
		school.setPostalcode(rs.getInt("postalcode"));
		school.setSuperceded(rs.getBoolean("superceded"));
		list.add(school);	
	
	}		
	} catch (SQLException e) {
	
	e.printStackTrace();}
finally
{
	try {conn.close();} catch (Exception ex) {}
}

return list;
	
}
	
	
	
}
