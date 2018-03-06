package com.tony.el.domain;

import java.util.Arrays;
import java.util.Date;

public class MessageIndex {

	private Date uploadedDate; 
	private String fileName; 
	private String checksum;
	private String refNumber;
	private String[] parameters; 
	private long dateStart; 
	private long dateStop;
	public Date getUploadedDate() {
		return uploadedDate;
	}
	public void setUploadedDate(Date uploadedDate) {
		this.uploadedDate = uploadedDate;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getChecksum() {
		return checksum;
	}
	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}
	public String getRefNumber() {
		return refNumber;
	}
	public void setRefNumber(String refNumber) {
		this.refNumber = refNumber;
	}
	public String[] getParameters() {
		return parameters;
	}
	public void setParameters(String[] parameters) {
		this.parameters = parameters;
	}
	public long getDateStart() {
		return dateStart;
	}
	public void setDateStart(long dateStart) {
		this.dateStart = dateStart;
	}
	public long getDateStop() {
		return dateStop;
	}
	public void setDateStop(long dateStop) {
		this.dateStop = dateStop;
	}
	
	@Override
	public String toString() {
		return "MessageIndex [uploadedDate=" + uploadedDate + ", fileName=" + fileName + ", checksum=" + checksum
				+ ", refNumber=" + refNumber + ", parameters=" + Arrays.toString(parameters) + ", dateStart="
				+ dateStart + ", dateStop=" + dateStop + "]";
	}
	
}
