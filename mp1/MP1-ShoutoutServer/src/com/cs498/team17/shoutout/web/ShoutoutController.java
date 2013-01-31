package com.cs498.team17.shoutout.web;

import java.util.logging.Logger;
import java.io.*;
import javax.servlet.http.*;
import com.cs498.team17.shoutout.s3torage.S3StorageManager;
import com.cs498.team17.shoutout.s3torage.Shoutout;


/**
 * This is the core of the Shoutout functionality.  
 * Most methods for loading and storing shoutouts are initiated in this class.
 */
public class ShoutoutController extends HttpServlet{
	
	private static final long serialVersionUID = 1L;
	private static final S3StorageManager storageMgr = new S3StorageManager();
	private static final Logger logger=Logger.getLogger(ShoutoutController.class.getName());
	
	
    /**
     * The post request handler that new shoutouts
     */
    public void doPost(HttpServletRequest req, HttpServletResponse resp) {
    	String name = req.getParameter("name");
    	String shout = req.getParameter("shout");
    	Shoutout so = new Shoutout(shout, name);
    	storageMgr.store(so, false, null);
    }
    
    /**
     * The get request handler that retrieves shoutouts
     */
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException{
    	resp.setContentType("text/html");
        PrintWriter out = resp.getWriter();

        String name = req.getParameter("name");
        out.println("<HTML>");
        out.println("<HEAD><TITLE>Hello, " + name + "</TITLE></HEAD>");
        out.println("<BODY>");
        out.println("Hello, " + name);
        out.println("</BODY></HTML>");
    }
	
}
