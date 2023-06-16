/**
 * 概要：取り込みデータのチェック
 *
 * 作成者：張＠ソフトテク
 * 作成日：2020/12/27
 */
package com.datainputbatch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CsvFieldCheck {

	static LogUtils log = new LogUtils();

	/**
     * 機能：給料データのチェック
     *
     * @param line　取り込みデータ
     * @param stmt　DB接続
     * @param inputMonth　対象年月
     * @return　true成功、false失敗。
     * @exception
     * @author 張＠ソフトテク
     */
	public static boolean chkData(String line,Statement stmt,String inputMonth) {

		String[] data = line.split(",", 0);
		int items = data.length;

		String employeeID = data[0];
		String month = data[1];
		String date = data[2];


		// 項目数取得
		if (!CsvFieldCheck.fieldItemChk(items,stmt)) {
			LogUtils.logError("取込中データ:" + line);
			return false;
		}

		// 年月チェック
		if (!CsvFieldCheck.fieldMonthChk(month)) {
			LogUtils.logError("取込中データ:" + line);
			return false;
		}

		// 将来チェック
		if(Integer.parseInt(month) > Integer.parseInt(inputMonth)) {
			LogUtils.logError("将来データので取り込まれません:[" + month + "]");
			LogUtils.logError("取込中データ:" + line);
		}

		// 日付チェック
		if (!CsvFieldCheck.fieldDateChk(date)) {
			LogUtils.logError("取込中データ:" + line);
			return false;
		}

		// 数値チェック
		if (!CsvFieldCheck.fieldNumChk(data)) {
			LogUtils.logError("取込中データ:" + line);
			return false;
		}

		if (!CsvFieldCheck.fieldExistChk(employeeID, month,stmt)) {
			LogUtils.logError("取込中データは既に存在しています。" + " 存在する社員ID:" + employeeID + " 年月:" + month +  " データ："+line);
			return false;
		}

		return true;

	}

	/**
	 * 項目数のチェック
	 *
	 * @param　items　取り込みデータの項目数
	 * @param　stmt　DB接続
	 *
	 * @return　TRUE成功、FALSE失敗
	 */
	private static boolean fieldItemChk(int items,Statement stmt) {

		// 項目数のチェック
		ResultSet rs;
		try {
			String sql = "select count(*) from information_schema.columns where table_schema='ems' and table_name='salaryinfo'";
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				int dblength = Integer.parseInt(rs.getString(1));
				if (items != dblength) {
					LogUtils.logError("データの項目数が正しくありません。項目数[" + items + "] " );
					return false;
				}
			}
			return true;
		} catch (SQLException e) {
			LogUtils.logError("システムエラーが発生しました。" +  e.toString());
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * 年月のチェック
	 *
	 * @param month 年月　YYYYMM
	 * @return　TRUE成功、FALSE失敗
	 */
	public static boolean fieldMonthChk(String month) {

		if (month == null || month.length() != 6) {
			LogUtils.logError("年月エラーが発生しました。[" + month + "]");
			return false;
		}

		month = month.trim();
		DateFormat df = new SimpleDateFormat("yyyyMM");

		// 日付/時刻解析を厳密に行うかどうかを設定する。
		df.setLenient(false);
		try {
			df.parse(month);
			return true;
		} catch (Exception e) {
			LogUtils.logError("年月エラーが発生しました。[" + month + "]");
			return false;
		}

	}

	/**
	 * 日付のチェック
	 *
	 * @param　date　日付
	 * @return　RUE成功、FALSE失敗
	 */
	public static boolean fieldDateChk(String date) {

		// 日付の書式を指定する
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		// 日付解析を厳密に行う設定にする
		df.setLenient(false);

		try {

			df.parse(date);
			return true;
		} catch (ParseException e) {
			// 日付妥当性NG時の処理
			LogUtils.logError("日付エラーが発生しました。[" + date + "]");
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * 数字のチェック
	 *
	 * @param　date 取込データ
	 * @return　TRUE成功、FALSE失敗
	 */

	public static boolean fieldNumChk(String[] data) {

		try {
			for (int i = 3; i < data.length; i++) {
				if(i==12 || i == 29) continue;
				Double.parseDouble(data[i]);
			}
		} catch (NumberFormatException e) {
			LogUtils.logError("日付エラーが発生しました。[" + data + "] " + e.toString());
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 【社員ID】及び【対象月】に重複データをチェックする
	 *
	 * @param　employeeID 社員ID
	 * @param　month 対象年月
	 * @param　stmt DB接続
	 *
	 * @return　RUE成功、FALSE失敗
	 */
	public static boolean fieldExistChk(String employeeID, String month,Statement stmt) {

		ResultSet rsslt;
		try {
			rsslt = stmt.executeQuery("select employeeID,month from ems.salaryinfo where employeeID='" + employeeID + "' and month='" + month +"'");
			// １行以上検索して来たらエラーにする。
			while (rsslt.next()) {
				String data1 = rsslt.getString("employeeID");
				if ( data1 != null) {
					LogUtils.logError("データが存在しています。[" + employeeID + "]：[" + month + "]");
					return false;
				}
			}

		} catch (SQLException e) {
			LogUtils.logError("システムエラーがしました。"+e.toString());
			e.printStackTrace();
			return false;
		}
		return true;
	}
}