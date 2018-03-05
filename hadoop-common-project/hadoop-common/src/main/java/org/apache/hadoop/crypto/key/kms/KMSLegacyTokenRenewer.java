package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.KMSUtil;

import java.io.IOException;

import static org.apache.hadoop.crypto.key.kms.KMSDelegationToken.TOKEN_LEGACY_KIND;

/**
 * The {@link KMSTokenRenewer} that supports legacy tokens.
 */
@InterfaceAudience.Private
@Deprecated
public class KMSLegacyTokenRenewer extends KMSTokenRenewer {

  @Override
  public boolean handleKind(Text kind) {
    return kind.equals(TOKEN_LEGACY_KIND);
  }

  /**
   * Create a key provider for token renewal / cancellation.
   * Caller is responsible for closing the key provider.
   */
  @Override
  protected KeyProvider createKeyProvider(Token<?> token,
      Configuration conf) throws IOException {
    assert token.getKind().equals(TOKEN_LEGACY_KIND);
    // Legacy tokens get service from configuration.
    return KMSUtil.createKeyProvider(conf,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
  }
}
