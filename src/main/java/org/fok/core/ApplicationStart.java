package org.fok.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.fok.core.datasource.BaseDatabaseAccess;

import com.google.protobuf.Message;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.oapi.scala.commons.SessionModules;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Slf4j
@Data
public class ApplicationStart extends SessionModules<Message> {
	@Override
	public String[] getCmds() {
		return new String[] { "___" };
	}

	@Override
	public String getModule() {
		return "CHAIN";
	}

	@Validate
	public void startup() {
		try {
			new Thread(new AccountStartThread()).start();
		} catch (Exception e) {
			log.error("dao注入异常", e);
		}
	}

	class AccountStartThread extends Thread {
		@Override
		public void run() {
			try {
//				while (dao == null || !dao.isReady()) {
//					log.debug("等待dao注入完成...");
//					Thread.sleep(1000);
//				}
			} catch (Exception e) {
				log.error("dao注入异常", e);
			}
		}

	}
}
