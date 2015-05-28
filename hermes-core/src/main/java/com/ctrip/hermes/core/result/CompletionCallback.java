package com.ctrip.hermes.core.result;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface CompletionCallback<T> {
	void onSuccess(T result);

	void onFailure(Throwable t);
}