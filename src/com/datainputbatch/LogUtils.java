/**
 * 概要：ログ処理
 *
 * 作成者：張＠ソフトテク
 * 作成日：2020/12/27
 */
package com.datainputbatch;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LogUtils {

    	public static Logger log = Logger.getLogger(LogUtils.class);
		private static Logger infoLogger = Logger.getLogger("InfoLogger");
	    private static Logger errorLogger = Logger.getLogger("ErrorLogger");
	    private static Logger warnLogger = Logger.getLogger("WarnLogger");
	    private static Logger debugLogger = Logger.getLogger("DebugLogger");

	    private enum LogType{
	        INFO,
	        ERROR,
	        WARN,
	        DEBUG,
	    }

	    /**
	     * 機能：基本情報取得
	     *
	     * @param
	     * @return
	     * @exception
	     * @author 張＠ソフトテク
	     */
	    private static String getMethodInfo() {

	    	// DEBUG用
	    	//PropertyConfigurator.configure("D:/pleiades/pleiades/workspace/CommonDataInput/src/com/datainputbatch/log4j.properties");
	    	PropertyConfigurator.configure("./resource/log4j.properties");
	        // ログ内容を出力する。
	        StackTraceElement traceElement = ((new Exception()).getStackTrace())[2];
	        StringBuffer stringBuffer = new StringBuffer("[").
	                append(traceElement.getFileName()).
	                append(" ( ").append(traceElement.getLineNumber()).append(" )| ").
	                append(traceElement.getMethodName()).
	                append("]:");
	        return stringBuffer.toString();
	    }

	    public static void logInfo(String logStr, Throwable...ex){
	        infoLogger.info(getMethodInfo()+logStr);
	    }

	    public static void logError(String logStr, Throwable...ex){
	        errorLogger.error(getMethodInfo()+logStr);
	    }

	    public static void logWarn(String logStr, Throwable...ex){
	        warnLogger.warn(getMethodInfo()+logStr);
	    }

	    public static void logDebug(String logStr, Throwable...ex){
	        if(debugLogger.isDebugEnabled()){
	            debugLogger.debug(getMethodInfo()+logStr);
	        }
	    }

}