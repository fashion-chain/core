package org.fok.core.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.core.model.Transaction.TransactionInput;
import org.fok.core.model.Transaction.TransactionOutput;

import com.google.protobuf.ByteString;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TransactionExecutorSeparator {
	int bucketSize = Runtime.getRuntime().availableProcessors();

	class RelationShip {
		Map<String, TransactionInfo> sequances = new HashMap<>();

		LinkedBlockingQueue<TransactionInfo> queue = new LinkedBlockingQueue<>();
	}

	public LinkedBlockingQueue<TransactionInfo> getTxnQueue(int index) {
		return buckets.get(index).queue;
	}

	public void reset() {
		if (buckets != null) {
			buckets.clear();
		} else {
			buckets = new ArrayList<>(bucketSize);
		}
		for (int i = 0; i < bucketSize; i++) {
			buckets.add(new RelationShip());
		}
	}

	List<RelationShip> buckets = new ArrayList<>();

	public String getBucketInfo() {
		StringBuffer sb = new StringBuffer("MIS.MTS,BucketSize=").append(buckets.size()).append(":[");
		for (int i = 0; i < bucketSize; i++) {
			RelationShip rs = buckets.get(i);
			if (i > 0) {
				sb.append(",");
			}
			sb.append(rs.sequances.size());
		}
		sb.append("]");
		return sb.toString();
	}

	public static int MAX_COMPARE_SIZE = 4;

	public String fastAddress(ByteString bstr) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < MAX_COMPARE_SIZE && i < bstr.size(); i++) {
			sb.append(bstr.byteAt(0));
		}
		return sb.toString();
	}

	public void doClearing(TransactionInfo[] oTransactionInfos) {
		int offset = 0;
		for (TransactionInfo tx : oTransactionInfos) {
			int bucketIdx = -1;
			for (int i = 0; i < bucketSize && bucketIdx < 0; i++) {
				RelationShip rs = buckets.get(i);
				// TODO bytes to hex
				if (buckets.get(i).sequances.containsKey(tx.getHash())) {
					bucketIdx = i;
					break;
				}
				if (bucketIdx < 0) {
					TransactionInput input = tx.getBody().getInput();
					if (rs.sequances.containsKey(fastAddress(input.getAddress()))) {
						bucketIdx = i;
						break;
					}
				}
				if (bucketIdx < 0) {
					for (TransactionOutput output : tx.getBody().getOutputsList()) {
						if (rs.sequances.containsKey(fastAddress(output.getAddress()))) {
							bucketIdx = i;
							break;
						}
					}
				}
			}
			if (bucketIdx < 0) {
				bucketIdx = offset;
				offset++;
			}
			RelationShip rs = buckets.get((bucketIdx) % bucketSize);
			for (TransactionOutput output : tx.getBody().getOutputsList()) {
				rs.sequances.put(fastAddress(output.getAddress()), tx);
			}
			rs.sequances.put(fastAddress(tx.getBody().getInput().getAddress()), tx);
			rs.queue.offer(tx);
		}
	}
}
