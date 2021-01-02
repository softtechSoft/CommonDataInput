/**
 * 概要：ファイル共通機能
 *
 * 作成者：張＠ソフトテク
 * 作成日：2020/12/27
 */
package com.datainputbatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtil {

	/**
     * 機能：エラーデータをバックアップフォルダーへ移動
     *
     * @param
     * @return
     * @exception
     * @author 張＠ソフトテク
     */
	public boolean bakErrFile() {
		String errorFolders = PropertyUtils.getProperty("ERR_DATAFOLDER");
		String errBakFolders = PropertyUtils.getProperty("BKERR_DATAFOLDER");

		//エラーファイルフォルダーは存在していない場合、エラーで終了。
		File errFolder = new File(errorFolders);
		if(!errFolder.exists()) {
			LogUtils.logError("エラーファイルのフォルダーが存在していません。Propertiesを確認してください。");
			return false;
		}

		// エラーのバックアップフォルダーは存在していない場合、エラーで終了。
		File errBakFolder = new File(errBakFolders);
		if(!errBakFolder.exists()) {
			LogUtils.logError("エラーファイルのバックアップフォルダーが存在していません。Propertiesを確認してください。");
			return false;
		}


		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String timeStamp = simpleDateFormat.format(new Date());

		// 全ファイルをバックする。
		File[] fileList = errFolder.listFiles();
		for (int i = 0; i < fileList.length; i++) {
			// エラーデータファイル
			File fl = fileList[i];

			// バックアックファイル名を生成する。
			String errFileName = fl.getName();
			String errBackName = errFileName.substring(0, errFileName.indexOf("."));
			errBackName = errBakFolders + "/" + errBackName + "_" + timeStamp + errFileName.substring(errFileName.indexOf(".")+1);

			// エラーファイルをバックアップする。
			File bkfl = new File(errBackName);
			fl.renameTo(bkfl);
		}

		return true;
	}

	/**
     * 機能：エラーデータをエラーファイルへ出力する。
     *
     * @param inputFileName 取り込みデータファイル名
     * @param line 取り込み行データ
     *
     * @return
     * @exception
     * @author 張＠ソフトテク
     */
	public boolean writeErrFile(String inputFileName,String line) {

		File inputFile = new File(inputFileName);
		// エラーファイル名
		String errFileName = PropertyUtils.getProperty("ERR_DATAFOLDER") + "/ERR_" + inputFile.getName();

		// エラーファイルへ追加する。
		File errFile = new File(errFileName);
		try {
			FileWriter fw = new FileWriter(errFile, true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(line);
			bw.close();
		} catch (IOException e) {
			LogUtils.logError("エラーファイルの作成にエラーが発生しました。"+ e);
			e.printStackTrace();
			return false;
		}

		return true;
	}
}
