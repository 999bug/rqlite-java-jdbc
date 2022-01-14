package com.ncst.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class FileStatus {
	private String sourcePath;
	private String relPath;
	private boolean trunk;
	private long trunkSize;
	private long lastUpdateTime;
	private String host;
	private int port;
	private long seqId;
	private long offset;
	private boolean draft;
	private boolean found = false;
	private String checksum;
	private String rluuid;
	private String bakDate;
	private boolean deleted;
	private String bakPath;

	private long skip;
	private short i2bbFileAction;

	public FileStatus() { }

	public FileStatus(String uuid, String sourcePath, String relPath, boolean trunk, long trunkSize, long lastUpdateTime, String host, long seqId, long offset, boolean draft, String checksum, String bakDate, boolean deleted, String bakPath) {
		this.rluuid = uuid;
		this.sourcePath = sourcePath;
		this.relPath = relPath;
		this.trunk = trunk;
		this.trunkSize = trunkSize;
		this.lastUpdateTime = lastUpdateTime;
		this.host = host;
		this.seqId = seqId;
		this.offset = offset;
		this.draft = draft;
		this.checksum = checksum;
		this.bakDate = bakDate;
		this.deleted = deleted;
		this.bakPath = bakPath;
	}

}
