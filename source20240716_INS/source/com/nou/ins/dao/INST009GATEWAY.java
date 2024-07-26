package com.nou.ins.dao;

import java.sql.Connection;
import java.util.Hashtable;
import java.util.Vector;

import javax.servlet.http.HttpSession;

import com.acer.apps.Page;
import com.acer.db.DBManager;
import com.acer.db.query.DBResult;
import com.acer.util.Utility;
import com.nou.UtilityX;

/*
 * (INST009) Gateway/*
 *-------------------------------------------------------------------------------*
 * Author    : ���      2017/02/24
 * Modification Log :
 * Vers     Date           By             Notes
 *--------- -------------- -------------- ----------------------------------------
 * V0.0.1   2017/02/24     ���           �إߵ{��
 *                                        �s�W getInst009ForUse(Hashtable ht)
 *--------------------------------------------------------------------------------
 */
public class INST009GATEWAY {

    /** ��ƱƧǤ覡 */
    private String orderBy = "";
    private DBManager dbmanager = null;
    private Connection conn = null;
    /* ���� */
    private int pageNo = 0;
    /** �C������ */
    private int pageSize = 0;

    /** �O���O�_���� */
    private boolean pageQuery = false;

    /** �ΨӦs�� SQL �y�k������ */
    private StringBuffer sql = new StringBuffer();

    /** <pre>
     *  �]�w��ƱƧǤ覡.
     *  Ex: "AYEAR, SMS DESC"
     *      ���H AYEAR �ƧǦA�H SMS �˧ǧǱƧ�
     *  </pre>
     */
    public void setOrderBy(String orderBy) {
        if(orderBy == null) {
            orderBy = "";
        }
        this.orderBy = orderBy;
    }

    /** ���o�`���� */
    public int getTotalRowCount() {
        return Page.getTotalRowCount();
    }

    /** �����\�إߪŪ����� */
    private INST009GATEWAY() {}

    /** �غc�l�A�d�ߥ�����ƥ� */
    public INST009GATEWAY(DBManager dbmanager, Connection conn) {
        this.dbmanager = dbmanager;
        this.conn = conn;
    }

    /** �غc�l�A�d�ߤ�����ƥ� */
    public INST009GATEWAY(DBManager dbmanager, Connection conn, int pageNo, int pageSize) {
        this.dbmanager = dbmanager;
        this.conn = conn;
        this.pageNo = pageNo;
        this.pageSize = pageSize;
        pageQuery = true;
    }

    /**
     * 
     * @param ht �����
     * @return �^�� Vector ����A���e�� Hashtable �����X�A<br>
     *         �C�@�� Hashtable �� KEY �����W�١AKEY ���Ȭ���쪺��<br>
     *         �Y����즳����W�١A�h�� KEY �Х[�W _NAME, EX: SMS �䤤�����г]�� SMS_NAME
     * @throws Exception
     */
    public Vector getInst009ForUse(Hashtable ht) throws Exception {
        if(ht == null) {
            ht = new Hashtable();
        }
        Vector result = new Vector();
        if(sql.length() > 0) {
            sql.delete(0, sql.length());
        }
        sql.append(
            "SELECT I09.ADD_INS_DATE, I09.IDNO, I09.NAME, I09.BIRTHDATE, I09.SEX, I09.FORMATTING, I09.INSUR_ID, I09.INSUR_ID_CK, I09.INS_NATION_MK, I09.MR_SALARY, I09.DR_SALARY, I09.SPECIAL_ID_TYPE, I09.LL_ID_SPECIAL, I09.RECV_SIP_TYPE, I09.SUBMIT_ID, I09.EMP_SUBMIT_RATE, I09.IND_SUBMIT_RATE, I09.LR_SUBMIT_DATE, I09.PEG, I09.INS_M_SALARY, I09.LIP_SUBMIT_AMT, I09.LIE_SUBMIT_AMT, I09.EIP_SUBMIT_AMT, I09.EIE_SUBMIT_AMT, I09.OII_SUBMIT_AMT, I09.PENSION_AMT, I09.SHL_PENSION_AMT, I09.WHT_CHKLIST_NO, I09.UPD_USER_ID, I09.UPD_DATE, I09.UPD_TIME, I09.UPD_MK, I09.ROWSTAMP, INS_R_SALARY, INS_STATUS, MEMO, DB_TABLE, INS_W_SALARY " +
            "FROM INST009 I09 " +
            "WHERE 1 = 1 "
        );
        if(!Utility.nullToSpace(ht.get("ADD_INS_DATE")).equals("")) {
            sql.append("AND I09.ADD_INS_DATE = '" + Utility.nullToSpace(ht.get("ADD_INS_DATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("IDNO")).equals("")) {
            sql.append("AND I09.IDNO = '" + Utility.nullToSpace(ht.get("IDNO")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("NAME")).equals("")) {
            sql.append("AND I09.NAME = '" + Utility.nullToSpace(ht.get("NAME")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("BIRTHDATE")).equals("")) {
            sql.append("AND I09.BIRTHDATE = '" + Utility.nullToSpace(ht.get("BIRTHDATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("SEX")).equals("")) {
            sql.append("AND I09.SEX = '" + Utility.nullToSpace(ht.get("SEX")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("FORMATTING")).equals("")) {
            sql.append("AND I09.FORMATTING = '" + Utility.nullToSpace(ht.get("FORMATTING")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INSUR_ID")).equals("")) {
            sql.append("AND I09.INSUR_ID = '" + Utility.nullToSpace(ht.get("INSUR_ID")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INSUR_ID_CK")).equals("")) {
            sql.append("AND I09.INSUR_ID_CK = '" + Utility.nullToSpace(ht.get("INSUR_ID_CK")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INS_NATION_MK")).equals("")) {
            sql.append("AND I09.INS_NATION_MK = '" + Utility.nullToSpace(ht.get("INS_NATION_MK")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("MR_SALARY")).equals("")) {
            sql.append("AND I09.MR_SALARY = '" + Utility.nullToSpace(ht.get("MR_SALARY")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("DR_SALARY")).equals("")) {
            sql.append("AND I09.DR_SALARY = '" + Utility.nullToSpace(ht.get("DR_SALARY")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("SPECIAL_ID_TYPE")).equals("")) {
            sql.append("AND I09.SPECIAL_ID_TYPE = '" + Utility.nullToSpace(ht.get("SPECIAL_ID_TYPE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("LL_ID_SPECIAL")).equals("")) {
            sql.append("AND I09.LL_ID_SPECIAL = '" + Utility.nullToSpace(ht.get("LL_ID_SPECIAL")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("RECV_SIP_TYPE")).equals("")) {
            sql.append("AND I09.RECV_SIP_TYPE = '" + Utility.nullToSpace(ht.get("RECV_SIP_TYPE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("SUBMIT_ID")).equals("")) {
            sql.append("AND I09.SUBMIT_ID = '" + Utility.nullToSpace(ht.get("SUBMIT_ID")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("EMP_SUBMIT_RATE")).equals("")) {
            sql.append("AND I09.EMP_SUBMIT_RATE = '" + Utility.nullToSpace(ht.get("EMP_SUBMIT_RATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("IND_SUBMIT_RATE")).equals("")) {
            sql.append("AND I09.IND_SUBMIT_RATE = '" + Utility.nullToSpace(ht.get("IND_SUBMIT_RATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("LR_SUBMIT_DATE")).equals("")) {
            sql.append("AND I09.LR_SUBMIT_DATE = '" + Utility.nullToSpace(ht.get("LR_SUBMIT_DATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("PEG")).equals("")) {
            sql.append("AND I09.PEG = '" + Utility.nullToSpace(ht.get("PEG")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INS_M_SALARY")).equals("")) {
            sql.append("AND I09.INS_M_SALARY = '" + Utility.nullToSpace(ht.get("INS_M_SALARY")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("LIP_SUBMIT_AMT")).equals("")) {
            sql.append("AND I09.LIP_SUBMIT_AMT = '" + Utility.nullToSpace(ht.get("LIP_SUBMIT_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("LIE_SUBMIT_AMT")).equals("")) {
            sql.append("AND I09.LIE_SUBMIT_AMT = '" + Utility.nullToSpace(ht.get("LIE_SUBMIT_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("EIP_SUBMIT_AMT")).equals("")) {
            sql.append("AND I09.EIP_SUBMIT_AMT = '" + Utility.nullToSpace(ht.get("EIP_SUBMIT_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("EIE_SUBMIT_AMT")).equals("")) {
            sql.append("AND I09.EIE_SUBMIT_AMT = '" + Utility.nullToSpace(ht.get("EIE_SUBMIT_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("OII_SUBMIT_AMT")).equals("")) {
            sql.append("AND I09.OII_SUBMIT_AMT = '" + Utility.nullToSpace(ht.get("OII_SUBMIT_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("PENSION_AMT")).equals("")) {
            sql.append("AND I09.PENSION_AMT = '" + Utility.nullToSpace(ht.get("PENSION_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("SHL_PENSION_AMT")).equals("")) {
            sql.append("AND I09.SHL_PENSION_AMT = '" + Utility.nullToSpace(ht.get("SHL_PENSION_AMT")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("WHT_CHKLIST_NO")).equals("")) {
            sql.append("AND I09.WHT_CHKLIST_NO = '" + Utility.nullToSpace(ht.get("WHT_CHKLIST_NO")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("UPD_USER_ID")).equals("")) {
            sql.append("AND I09.UPD_USER_ID = '" + Utility.nullToSpace(ht.get("UPD_USER_ID")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("UPD_DATE")).equals("")) {
            sql.append("AND I09.UPD_DATE = '" + Utility.nullToSpace(ht.get("UPD_DATE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("UPD_TIME")).equals("")) {
            sql.append("AND I09.UPD_TIME = '" + Utility.nullToSpace(ht.get("UPD_TIME")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("UPD_MK")).equals("")) {
            sql.append("AND I09.UPD_MK = '" + Utility.nullToSpace(ht.get("UPD_MK")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("ROWSTAMP")).equals("")) {
            sql.append("AND I09.ROWSTAMP = '" + Utility.nullToSpace(ht.get("ROWSTAMP")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INS_R_SALARY")).equals("")) {
            sql.append("AND I09.INS_R_SALARY = '" + Utility.nullToSpace(ht.get("INS_R_SALARY")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INS_STATUS")).equals("")) {
            sql.append("AND I09.INS_STATUS = '" + Utility.nullToSpace(ht.get("INS_STATUS")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("MEMO")).equals("")) {
            sql.append("AND I09.MEMO = '" + Utility.nullToSpace(ht.get("MEMO")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("DB_TABLE")).equals("")) {
            sql.append("AND I09.DB_TABLE = '" + Utility.nullToSpace(ht.get("DB_TABLE")) + "' ");
        }
        if(!Utility.nullToSpace(ht.get("INS_W_SALARY")).equals("")) {
            sql.append("AND I09.INS_W_SALARY = '" + Utility.nullToSpace(ht.get("INS_W_SALARY")) + "' ");
        }

        if(!orderBy.equals("")) {
            String[] orderByArray = orderBy.split(",");
            for(int i = 0; i < orderByArray.length; i++) {
                orderByArray[i] = "I09." + orderByArray[i].trim();

                if(i == 0) {
                    sql.append("ORDER BY ");
                } else {
                    sql.append(", ");
                }
                sql.append(orderByArray[i].trim().toUpperCase());
            }
            orderBy = "";
        }

        DBResult rs = null;
        try {
            if(pageQuery) {
                // �̤������X���
                rs = Page.getPageResultSet(dbmanager, conn, sql.toString(), pageNo, pageSize);
            } else {
                // ���X�Ҧ����
                rs = dbmanager.getSimpleResultSet(conn);
                rs.open();
                rs.executeQuery(sql.toString());
            }
            Hashtable rowHt = null;
            while (rs.next()) {
                rowHt = new Hashtable();
                /** �N���ۤ@���L�h */
                for (int i = 1; i <= rs.getColumnCount(); i++)
                    rowHt.put(rs.getColumnName(i), rs.getString(i));

                result.add(rowHt);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        return result;
    }

	public Vector GetIns007Calc(Hashtable ht, HttpSession session)
			throws Exception {
		Vector result = new Vector();
		DBResult rs = null;
		StringBuffer sql = new StringBuffer();
		INST005GATEWAY inst005 = new INST005GATEWAY(dbmanager, conn);
		INST004GATEWAY inst004 = new INST004GATEWAY(dbmanager, conn);
		try {

			// ���R�� �w��n�����
			INST009DAO inst009 = new INST009DAO(this.dbmanager, this.conn);
			int cnt = inst009.delete("ADD_INS_DATE >= '"
					+ Utility.nullToSpace(ht.get("ADD_INS_DATE_S"))
					+ "' AND ADD_INS_DATE <= '"
					+ Utility.nullToSpace(ht.get("ADD_INS_DATE_E")) + "'");
			sql.delete(0, sql.length());

			// �d�߸��
			sql.append("SELECT M.SALARY_DATE AS ADD_INS_DATE, ");
			sql.append("         R1.IDNO, ");
			sql.append("         R2.NAME, ");
			sql.append("         CASE WHEN LENGTH(R2.BIRTHDATE) = 8 THEN LPAD(TO_NUMBER(substr(R2.BIRTHDATE,1,4))-1911,3,'0') || SUBSTR(R2.BIRTHDATE,5,4) ELSE N'' END AS INS_BIRTHDATE, ");
			sql.append("         DECODE (R2.SEX,  '1', 'M',  '2', 'F') AS INS_SEX, ");
			sql.append("         R1.INS_ID_TYPE, ");
			sql.append("         DECODE(R1.INS_ID_TYPE,'15','2','22','2','1') AS FORMATTING, ");
			sql.append("         SUBSTR (R3.CODE_NAME, 1, 8) AS INSUR_ID, ");
			sql.append("         SUBSTR (R3.CODE_NAME, 9, 1) AS INSUR_ID_CK, ");
			sql.append("         DECODE (R1.INS_ID_SPECIAL,  '1', 'Y',  '2', '1',  '3', '2',  NULL) ");
			sql.append("            AS INS_NATION_MK, ");
			//sql.append("         SUM (M.PAYABLE_AMT) * 30 AS MR_SALARY, ");
			sql.append("         SUM (M.PAYABLE_AMT) AS MR_SALARY, ");  /*20240716 �H����~�ꬰ���~���O�ŶZ�p��O�O*/
			sql.append("         SUM (M.PAYABLE_AMT) AS DR_SALARY, ");
			sql.append("         '1' AS SPECIAL_ID_TYPE, ");
			// sql.append("         CASE ");
			// sql.append("            WHEN R4.INS_KIND = '4' ");
			// sql.append("            THEN ");
			// sql.append("               DECODE (R1.MAJWORK_KIND,  '1', 'H',  '2', 'I') ");
			// sql.append("            ELSE ");
			// sql.append("               'Y' ");
			// sql.append("         END ");
			// sql.append("            AS LL_ID_SPECIAL, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' THEN 'F' ELSE NULL END AS LL_ID_SPECIAL, ");
			sql.append("         DECODE (R1.INS_ID_TYPE,  '15', '1',  '06', '2',  NULL) ");
			sql.append("            AS RECV_SIP_TYPE, ");
			// sql.append("         CASE ");
			// sql.append("            WHEN R4.INS_KIND = '4' ");
			// sql.append("            THEN ");
			// sql.append("               DECODE (R1.MAJWORK_KIND,  '1', '2',  '2', '1') ");
			// sql.append("            ELSE ");
			// sql.append("               NULL ");
			// sql.append("         END ");
			// sql.append("            AS SUBMIT_ID, ");
			sql.append(" CASE WHEN R1.INS_ID_TYPE IN ('03','11','12','13','14','15','17','18','19','20','21','22') THEN CASE WHEN R1.PENSION_RATE > 0 THEN '2' ELSE '1' END ELSE NULL END AS SUBMIT_ID, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' THEN R5.US_RATIO ELSE NULL END ");
			sql.append("            AS EMP_SUBMIT_RATE, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' AND NVL(R1.PENSION_RATE,0) > 0 THEN R1.PENSION_RATE ELSE NULL END ");
			sql.append("            AS IND_SUBMIT_RATE, ");
			sql.append("         NULL AS LR_SUBMIT_DATE ");
			sql.append("    ,R1.INS_ID_SPECIAL, R1.HANDICAP_GRADE ");
			sql.append("    FROM INST008 M ");
			sql.append("         JOIN INST003 R1 ");
			sql.append("            ON     R1.AYEAR = M.AYEAR ");
			sql.append("               AND R1.SMS = M.SMS ");
			sql.append("               AND R1.IDNO = M.IDNO ");
			sql.append("               AND R1.INS_METHOD = '1' ");
			sql.append("         JOIN TRAT001 R2 ON R2.IDNO = R1.IDNO ");
			sql.append("         JOIN SYST001 R3 ON R3.KIND = 'INS_D_UR_ID' AND R3.CODE = 'D' ");
			sql.append("         LEFT JOIN INST004 R4 ");
			sql.append("            ON R4.INS_ID_TYPE = R1.INS_ID_TYPE AND R4.INS_KIND = '4' ");
			sql.append("         JOIN INST006 R5 ON R5.INS_KIND = '4' ");
			sql.append("   WHERE 1=1 AND NVL(M.USE_STATUS,'1') = '1' ");
			sql.append(" AND M.PAYABLE_AMT > 0 ");
			sql.append("   AND R1.INS_ID_TYPE NOT IN ('01','02','04','05','06') ");//����z�ҫO�[�O�δ��������Ҥu�h���
			if (!Utility.nullToSpace(ht.get("ADD_INS_DATE_S")).equals("")) {
				sql.append("AND M.SALARY_DATE >= '"
						+ Utility.nullToSpace(ht.get("ADD_INS_DATE_S")) + "' ");
			}
			if (!Utility.nullToSpace(ht.get("ADD_INS_DATE_E")).equals("")) {
				sql.append("AND M.SALARY_DATE <= '"
						+ Utility.nullToSpace(ht.get("ADD_INS_DATE_E")) + "' ");
			}
			sql.append("GROUP BY M.SALARY_DATE, ");
			sql.append("         R1.IDNO, ");
			sql.append("         R2.NAME, ");
			sql.append("         R2.BIRTHDATE, ");
			sql.append("         DECODE (R2.SEX,  '1', 'M',  '2', 'F'), ");
			sql.append("         R1.INS_ID_TYPE, ");
			sql.append("         SUBSTR (R3.CODE_NAME, 1, 8), ");
			sql.append("         SUBSTR (R3.CODE_NAME, 9, 1), ");
			sql.append("         DECODE (R1.INS_ID_SPECIAL,  '1', 'Y',  '2', '1',  '3', '2',  NULL), ");
			// sql.append("         CASE ");
			// sql.append("            WHEN R4.INS_KIND = '4' ");
			// sql.append("            THEN ");
			// sql.append("               DECODE (R1.MAJWORK_KIND,  '1', 'H',  '2', 'I') ");
			// sql.append("            ELSE ");
			// sql.append("               'Y' ");
			// sql.append("         END, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' THEN 'F' ELSE NULL END, ");
			sql.append("         DECODE (R1.INS_ID_TYPE,  '15', '1',  '06', '2',  NULL), ");
			// sql.append("         CASE ");
			// sql.append("            WHEN R4.INS_KIND = '4' ");
			// sql.append("            THEN ");
			// sql.append("               DECODE (R1.MAJWORK_KIND,  '1', '2',  '2', '1') ");
			// sql.append("            ELSE ");
			// sql.append("               NULL ");
			// sql.append("         END, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' THEN R5.US_RATIO ELSE NULL END, ");
			sql.append("         CASE WHEN R4.INS_KIND = '4' AND NVL(R1.PENSION_RATE,0) > 0 THEN R1.PENSION_RATE ELSE NULL END, ");
			sql.append("         CASE WHEN R1.INS_ID_TYPE IN ('03','11','12','13','14','15','17','18','19','20','21','22') THEN CASE WHEN R1.PENSION_RATE > 0 THEN '2' ELSE '1' END ELSE NULL END ");
			sql.append("    ,R1.INS_ID_SPECIAL, R1.HANDICAP_GRADE ");
			if (pageQuery) {
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}
			while (rs.next()) {
				String ADD_INS_DATE = rs.getString("ADD_INS_DATE");
				String MR_SALARY = rs.getString("MR_SALARY");
				String INS_M_SALARY = "";// �ҫO��O�ŶZ���B
				String INS_W_SALARY = "";// ¾�@��O�ŶZ���B
				String INS_R_SALARY = "";// �Ұh��O�ŶZ���B
				// ���o�ҫO��O�ŶZ
				Hashtable whereHT = new Hashtable();
				whereHT.put("EXE_DATE_S", rs.getString("ADD_INS_DATE"));
				whereHT.put("INS_KIND", "1");
				whereHT.put("SALARY", MR_SALARY);
				inst005.setOrderBy("EXE_DATE DESC");
				Vector vtInst005 = inst005.getInst005ForUse(whereHT);
				if (vtInst005.size() > 0) {
					Hashtable htInst005 = (Hashtable) vtInst005.get(0);
					INS_M_SALARY = String.valueOf(htInst005.get("M_SALARY"));
				}
				if (INS_M_SALARY.length() == 0) {
					throw new Exception("�ҫO��O�ŶZ�|���]�w TABLE=INST005, EXE_DATE="
							+ rs.getString("ADD_INS_DATE")
							+ "�AINS_KIND=1�ASALARY=" + MR_SALARY);
				}
				// ���o¾�@��O�ŶZ
				whereHT = new Hashtable();
				whereHT.put("EXE_DATE_S", rs.getString("ADD_INS_DATE"));
				whereHT.put("INS_KIND", "3");
				whereHT.put("SALARY", MR_SALARY);
				inst005.setOrderBy("EXE_DATE DESC");
				vtInst005 = inst005.getInst005ForUse(whereHT);
				if (vtInst005.size() > 0) {
					Hashtable htInst005 = (Hashtable) vtInst005.get(0);
					INS_W_SALARY = String.valueOf(htInst005.get("M_SALARY"));
				}
				if (INS_W_SALARY.length() == 0) {
					throw new Exception("¾�@��O�ŶZ�|���]�w TABLE=INST005, EXE_DATE="
							+ rs.getString("ADD_INS_DATE")
							+ "�AINS_KIND=3�ASALARY=" + MR_SALARY);
				}
				// ���o�Ұh��O�ŶZ
				whereHT = new Hashtable();
				whereHT.put("EXE_DATE_S", rs.getString("ADD_INS_DATE"));
				whereHT.put("INS_KIND", "4");
				whereHT.put("SALARY", MR_SALARY);
				inst005.setOrderBy("EXE_DATE DESC");
				vtInst005 = inst005.getInst005ForUse(whereHT);
				if (vtInst005.size() > 0) {
					Hashtable htInst005 = (Hashtable) vtInst005.get(0);
					INS_R_SALARY = String.valueOf(htInst005.get("M_SALARY"));
				}
				if (INS_R_SALARY.length() == 0) {
					throw new Exception("�Ұh��O�ŶZ�|���]�w TABLE=INST005, EXE_DATE="
							+ rs.getString("ADD_INS_DATE")
							+ "�AINS_KIND=4�ASALARY=" + MR_SALARY);
				}
				
				// �ˬd�O�_�����߻�ê
				String INS_ID_SPECIAL = rs.getString("INS_ID_SPECIAL");
				String HANDICAP_GRADE = rs.getString("HANDICAP_GRADE");
				double HANDICAP_RATE = 1;
				if ("5".equals(INS_ID_SPECIAL)) {
					if ("01".equals(HANDICAP_GRADE)) {
						HANDICAP_RATE = 0.75f; // ����
					} else if ("02".equals(HANDICAP_GRADE)) {
						HANDICAP_RATE = 0.5f;// ����
					} else if ("03".equals(HANDICAP_GRADE)
							|| "04".equals(HANDICAP_GRADE)) {
						HANDICAP_RATE = 0f;// ���סA������
					}
				}

				// �p��ҫO��ú���B
				whereHT =  new Hashtable();
				whereHT.put("ADD_INS_YEAR", ADD_INS_DATE.substring(0, 4));
				whereHT.put("INS_ID_TYPE", rs.getString("INS_ID_TYPE"));
				whereHT.put("INS_KIND", "1");
				Vector vtInst004 = inst004.getIns007Query(whereHT);
				String LIP_SUBMIT_AMT = "";// �ӤH��ú�O
				String LIE_SUBMIT_AMT = "";// �U�D��ú�O
				for (int i = 0; i < vtInst004.size(); i++) {
					Hashtable htInst004 = (Hashtable) vtInst004.get(i);
					String RATE = Utility.checkNull(htInst004.get("RATE"), "0");
					String PS_RATIO = Utility.checkNull(
							htInst004.get("PS_RATIO"), "0");
					String US_RATIO = Utility.checkNull(
							htInst004.get("US_RATIO"), "0");



					LIP_SUBMIT_AMT = String.valueOf((Double
							.parseDouble(INS_M_SALARY)
							* Double.parseDouble(RATE) * Double
							.parseDouble(PS_RATIO)) / 30);
					LIP_SUBMIT_AMT = String.valueOf(Double
							.parseDouble(LIP_SUBMIT_AMT) * HANDICAP_RATE);

					LIE_SUBMIT_AMT = String.valueOf((Double
							.parseDouble(INS_M_SALARY)
							* Double.parseDouble(RATE) * Double
							.parseDouble(US_RATIO)) / 30);
				}

				// �p��N�O��ú���B
				whereHT = new Hashtable();
				whereHT.put("ADD_INS_YEAR", ADD_INS_DATE.substring(0, 4));
				whereHT.put("INS_ID_TYPE", rs.getString("INS_ID_TYPE"));
				whereHT.put("INS_KIND", "2");
				vtInst004 = inst004.getIns007Query(whereHT);
				String EIP_SUBMIT_AMT = "";// �ӤH��ú�O
				String EIE_SUBMIT_AMT = "";// �U�D��ú�O
				for (int i = 0; i < vtInst004.size(); i++) {
					Hashtable htInst004 = (Hashtable) vtInst004.get(i);
					String RATE = Utility.checkNull(htInst004.get("RATE"), "0");
					String PS_RATIO = Utility.checkNull(
							htInst004.get("PS_RATIO"), "0");
					String US_RATIO = Utility.checkNull(
							htInst004.get("US_RATIO"), "0");

					EIP_SUBMIT_AMT = String.valueOf((Double
							.parseDouble(INS_M_SALARY)
							* Double.parseDouble(RATE) * Double
							.parseDouble(PS_RATIO)) / 30);
					EIP_SUBMIT_AMT = String.valueOf(Double
							.parseDouble(EIP_SUBMIT_AMT) * HANDICAP_RATE);

					EIE_SUBMIT_AMT = String.valueOf((Double
							.parseDouble(INS_M_SALARY)
							* Double.parseDouble(RATE) * Double
							.parseDouble(US_RATIO)) / 30);
				}
				
				// �p��¾�a��ú���B OII_SUBMIT_AMT
				whereHT = new Hashtable();
				whereHT.put("ADD_INS_YEAR", ADD_INS_DATE.substring(0, 4));
				whereHT.put("INS_ID_TYPE", rs.getString("INS_ID_TYPE"));
				whereHT.put("INS_KIND", "3");
				vtInst004 = inst004.getIns007Query(whereHT);
				String OII_SUBMIT_AMT = "";// �U�D��ú�O
				for (int i = 0; i < vtInst004.size(); i++) {
					Hashtable htInst004 = (Hashtable) vtInst004.get(i);
					String RATE = Utility.checkNull(htInst004.get("RATE"), "0");
					OII_SUBMIT_AMT = String.valueOf(Double
							.parseDouble(INS_W_SALARY)
							* Double.parseDouble(RATE)/ 30);
				}
				
				//�p��Ұh��ú���B
				String SUBMIT_ID = rs.getString("SUBMIT_ID");
				String IND_SUBMIT_RATE = rs.getString("IND_SUBMIT_RATE");
				String EMP_SUBMIT_RATE = rs.getString("EMP_SUBMIT_RATE");
				String PENSION_AMT  ="";//�ӤH��ú�O
				String SHL_PENSION_AMT = "";//�U�D��ú�O
				if (SUBMIT_ID.length()>0){
					if (INS_R_SALARY.length() > 0
							&& IND_SUBMIT_RATE.length() > 0) {
						PENSION_AMT = String.valueOf(Double
								.parseDouble(INS_R_SALARY)
								* Double.parseDouble(IND_SUBMIT_RATE) / 30);
					}
					if (INS_R_SALARY.length() > 0
							&& EMP_SUBMIT_RATE.length() > 0) {
						SHL_PENSION_AMT = String.valueOf(Double
								.parseDouble(INS_R_SALARY)
								* Double.parseDouble(EMP_SUBMIT_RATE) / 30);
					}

				}

				// �N��Ƽg�J�ҫO�Ұh�G�X�@�[�O��(INST009)
				Hashtable insertInst009Ht = new Hashtable();
				insertInst009Ht.put("ADD_INS_DATE", ADD_INS_DATE);
				insertInst009Ht.put("IDNO", rs.getString("IDNO"));
				insertInst009Ht.put("NAME", rs.getString("NAME"));
				insertInst009Ht.put("INS_BIRTHDATE", rs.getString("INS_BIRTHDATE"));
				insertInst009Ht.put("INS_SEX", rs.getString("INS_SEX"));
				insertInst009Ht.put("FORMATTING", rs.getString("FORMATTING"));
				insertInst009Ht.put("INSUR_ID", rs.getString("INSUR_ID"));
				insertInst009Ht.put("INSUR_ID_CK", rs.getString("INSUR_ID_CK"));
				insertInst009Ht.put("INS_NATION_MK", rs.getString("INS_NATION_MK"));
				insertInst009Ht.put("MR_SALARY", rs.getString("MR_SALARY"));
				insertInst009Ht.put("DR_SALARY", rs.getString("DR_SALARY"));
				insertInst009Ht.put("SPECIAL_ID_TYPE", rs.getString("SPECIAL_ID_TYPE"));
				insertInst009Ht.put("LL_ID_SPECIAL", rs.getString("LL_ID_SPECIAL"));
				insertInst009Ht.put("RECV_SIP_TYPE", rs.getString("RECV_SIP_TYPE"));
				insertInst009Ht.put("SUBMIT_ID", SUBMIT_ID);
				insertInst009Ht.put("EMP_SUBMIT_RATE", rs.getString("EMP_SUBMIT_RATE"));
				insertInst009Ht.put("IND_SUBMIT_RATE", rs.getString("IND_SUBMIT_RATE"));
				insertInst009Ht.put("LR_SUBMIT_DATE", rs.getString("LR_SUBMIT_DATE"));
				// insertInst009Ht.put("PEG", );
				insertInst009Ht.put("INS_M_SALARY", INS_M_SALARY);
				insertInst009Ht.put("INS_R_SALARY", INS_R_SALARY);
				insertInst009Ht.put("INS_W_SALARY", INS_W_SALARY);
				insertInst009Ht.put("LIP_SUBMIT_AMT", LIP_SUBMIT_AMT);
				insertInst009Ht.put("LIE_SUBMIT_AMT", LIE_SUBMIT_AMT);
				insertInst009Ht.put("EIP_SUBMIT_AMT", EIP_SUBMIT_AMT);
				insertInst009Ht.put("EIE_SUBMIT_AMT", EIE_SUBMIT_AMT);
				insertInst009Ht.put("OII_SUBMIT_AMT", OII_SUBMIT_AMT);
				insertInst009Ht.put("PENSION_AMT", PENSION_AMT);
				insertInst009Ht.put("SHL_PENSION_AMT", SHL_PENSION_AMT);
				// insertInst009Ht.put("WHT_CHKLIST_NO", );
				insertInst009Ht.put("UPD_MK", "1");
				insertInst009Ht.put("DB_TABLE", UtilityX.getSqlDataForString(dbmanager, conn, "select unique db_table from inst008 where idno='"+rs.getString("IDNO")+"' and salary_date = '"+ADD_INS_DATE+"'", ""));
				inst009 = new INST009DAO(dbmanager, conn,
						insertInst009Ht, session);
				inst009.insert();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null)
				rs.close();
		}

		return result;
	}
}
