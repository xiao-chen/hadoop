package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.KMSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_KIND;

/**
 * The KMS implementation of {@link TokenRenewer}.
 */
@InterfaceAudience.Private
public class KMSTokenRenewer extends TokenRenewer {

  protected static final Logger LOG =
      LoggerFactory
          .getLogger(org.apache.hadoop.crypto.key.kms.KMSTokenRenewer.class);

  @Override
  public boolean handleKind(Text kind) {
    return kind.equals(TOKEN_KIND);
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException {
    LOG.debug("Renewing delegation token {}", token);
    KeyProvider keyProvider = createKeyProvider(token, conf);
    try {
      if (!(keyProvider instanceof
          KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
        LOG.warn("keyProvider {} cannot renew token {}.",
            keyProvider == null ? "null" : keyProvider.getClass(), token);
        return 0;
      }
      return ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
          keyProvider).renewDelegationToken(token);
    } finally {
      if (keyProvider != null) {
        keyProvider.close();
      }
    }
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException {
    LOG.debug("Canceling delegation token {}", token);
    KeyProvider keyProvider = createKeyProvider(token, conf);
    try {
      if (!(keyProvider instanceof
          KeyProviderDelegationTokenExtension.DelegationTokenExtension)) {
        LOG.warn("keyProvider {} cannot cancel token {}.",
            keyProvider == null ? "null" : keyProvider.getClass(), token);
        return;
      }
      ((KeyProviderDelegationTokenExtension.DelegationTokenExtension)
          keyProvider).cancelDelegationToken(token);
    } finally {
      if (keyProvider != null) {
        keyProvider.close();
      }
    }
  }

  /**
   * Create a key provider for token renewal / cancellation.
   * Caller is responsible for closing the key provider.
   */
  protected KeyProvider createKeyProvider(Token<?> token,
      Configuration conf) throws IOException {
    return KMSUtil
        .createKeyProviderFromTokenService(conf, token.getService().toString());
  }
}
