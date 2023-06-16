/**
 * 概要：給料データを取り込み
 *
 * 作成者：張＠ソフトテク
 * 作成日：2020/12/27
 */
package com.datainputbatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CommonDataInput {

	String inputFile="";

	/**
     * 機能：給料データ取り込みメインロジック
     *
     * @param args[0]：対象年月、args[1]：対象データファイル名
     * @return
     * @exception
     * @author 張＠ソフトテク
     */
	public static void main(String[] args) {
		LogUtils.logInfo("給料データ取込開始...");

		// DEBUG用
		args = new String[2];
		args[0] = "202012";
		args[1] = "D:/InputData/salaryInfo.csv";

		// パラメータ数判断
		if (args == null || args.length != 2) {
			// パラメータがない場合、コマンドプロンプトへ実行方法を提示する。
			System.out.println("******************************************************");
			System.out.println("*パラメータが必要です。                            **" );
			System.out.println("* コマンド：                                       **" );
			System.out.println("*   バッチ名 対象年月 対象データ                   **" );
			System.out.println("*  例： CommonDataInput 202012 /data/datainput.csv **" );
			System.out.println("*****************************************************");
			// エラーで終了。
			System.exit(1);
		}

		// 対象年月判断
		String inputMonth = args[0];
		if (!CsvFieldCheck.fieldMonthChk(inputMonth)) {
			System.out.println("対象年月は不正です。再入力してください。");
			// エラーで終了。
			System.exit(1);
		}

		// ファイルの存在を確認する
		String inputFile = args[1];
		File dataFile = new File(inputFile);
        if (!dataFile.exists()) {
            System.out.println("ファイルが存在していません。再入力してください。");
            // エラーで終了。
         	System.exit(1);
        }

        // 必要な設定をチェックする
        String driverClass=PropertyUtils.getProperty("DRIVER_CLASS");
		String dbURL = PropertyUtils.getProperty("DB_URL");
		String dbUser = PropertyUtils.getProperty("DB_USER");
		String dbPswd = PropertyUtils.getProperty("DB_PASSWORD");
		if(driverClass == null || dbURL==null || dbUser == null || dbPswd == null) {
			System.out.println("DB情報を設定してください。");
            // エラーで終了。
         	System.exit(1);
		}
		// フォルダーの指定は必須。
		if(PropertyUtils.getProperty("ERR_DATAFOLDER")==null || PropertyUtils.getProperty("BKERR_DATAFOLDER")==null) {
			System.out.println("エラーファイルおよびバックアップ用フォルダーを設定してください。");
            // エラーで終了。
         	System.exit(1);
		}

        // 前回エラーデータをバックアップ
        FileUtil fileUtil = new FileUtil();
        if(!fileUtil.bakErrFile()) {
        	// エラーで終了。
         	System.exit(1);
        }

		try {
	        // DBへ接続する。
			// インストールしたMySQLのドライバを指定
			Class.forName(driverClass);

			// MySQLデータベースに接続 (DB名,ID,パスワードを指定)
			Connection conn = DriverManager.getConnection(dbURL,dbUser,dbPswd);
			Statement stmt = conn.createStatement();

			// csvファイル読み込み
			CommonDataInput commDataInput = new CommonDataInput();
			commDataInput.inputData(stmt,inputFile,inputMonth);

			// DB接続をクローズ
			stmt.close();
			conn.close();

		} catch (ClassNotFoundException e) {
			LogUtils.logError("DB実行エラーが発生しました。" + e);
			e.printStackTrace();
			// エラーで終了。
         	System.exit(1);
		} catch (SQLException e) {
			LogUtils.logError("DB実行エラーが発生しました。" + e);
			e.printStackTrace();
			// エラーで終了。
         	System.exit(1);
		}

		//正常終了。
		LogUtils.logInfo("給料データ取込終了...");
		System.exit(0);

	}

	/**
     * 機能：給料データ取り込み
     *
     * @param stmt　DB接続
     * @param dataFileName　データファイル
     * @param inputMonth　対象年月
     * @return　true成功、false失敗。
     * @exception
     * @author 張＠ソフトテク
     */
	private void inputData(Statement stmt,String dataFileName,String inputMonth ) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFileName), "UTF-8"));
			String line;

			CommonDataInput commDataInput = new CommonDataInput();
			FileUtil fileUtil = new FileUtil();

			// 1行ずつCSVファイルを読み込
			while ((line = br.readLine()) != null) {

				//空行を飛ばす
				if (line.length()==0 ) continue;
				// データチェック
				if (CsvFieldCheck.chkData(line,stmt,inputMonth)) {
					//DBへ書き込み
					String sql = commDataInput.createSQL(line);
					stmt.executeUpdate(sql);
				} else {
					fileUtil.writeErrFile(dataFileName,line);
				}

			}
			// ファイルクローズ
			br.close();
		} catch ( IOException | SQLException e1) {
			LogUtils.logError("DB実行エラーが発生しました。" + e1);
			e1.printStackTrace();
		}

	}

	/**
     * 機能：給料情報テーブルへ挿入SQLの作成
     *
     * @param line 取り込みデータ（行）
     * @return　SQL文
     * @exception
     * @author 張＠ソフトテク
     */
	private String createSQL(String line ) {

		String[] data = line.split(",", 0);
		String sql = "INSERT INTO ems.SALARYINFO ("
				+ "employeeID, "
				+ "month, "
				+ "paymentDate, "
				+ "base, "
				+ "overTime, "
				+ "shortage, "
				+ "overTimePlus, "
				+ "shortageReduce, "
				+ "transportExpense, "
				+"specialAddition,"
				+ "allowancePlus, "
				+ "allowanceReduce, "
				+ "allowanceReason, "
				+"welfarePensionSelf,"
				+"welfarePensionComp,"
				+ "welfareHealthSelf, "
				+ "welfareHealthComp, "
				+ "welfareBaby, "
				+ "eplyInsSelf, "
				+ "eplyInsComp, "
				+ "eplyInsWithdraw, "
				+"wkAcccpsIns,"
				+ "withholdingTax, "
				+ "municipalTax, "
				+ "rental, "
				+ "rentalMgmtFee, "
				+"specialReduce,"
				+ "sum,"
				+"totalFee,"
				+"remark,"
				+ "deleteFlg,"
				+ "insertDate,"
				+ "updateDate) VALUES "
				+ "('"+ data[0] + "','"
				+ data[1] + "','"
				+ data[2] + "','"
				+ data[3] + "','"
				+ data[4] + "','"
				+ data[5] + "','"
				+ data[6] +"','"
				+ data[7] +"','"
				+ data[8] + "','"
				+ data[9] +"','"
				+ data[10] + "','"
				+ data[11] + "','"
				+ data[12] + "','"
				+ data[13] + "','"
				+ data[14] +"','"
				+ data[15] + "','"
				+ data[16] + "','"
				+ data[17] + "','"
				+ data[18]+ "','"
				+ data[19]+ "','"
				+ data[20] + "','"
				+ data[21] + "','"
				+ data[22] + "','"
				+ data[23]+ "','"
				+ data[24] + "','"
				+ data[25] +"','"
				+ data[26] + "','"
				+ data[27] + "','"
				+ data[28]+ "','"
				+ data[29] + "','"
				+ data[30] +"','"
				+ data[31] + "','"
				+ data[32] + "','0')";

		return sql;
	}
}


