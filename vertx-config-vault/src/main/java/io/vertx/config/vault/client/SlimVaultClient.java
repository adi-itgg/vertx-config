/*
 * Copyright (c) 2014 Red Hat, Inc. and others
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.vertx.config.vault.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A very simple Vault client - does not intend to be complete.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class SlimVaultClient {

  public static final String TOKEN_HEADER = "X-Vault-Token";
  private final WebClient client;
  private String token;

  /**
   * Creates an instance of {@link SlimVaultClient}.
   *
   * @param vertx         the vert.x instance
   * @param configuration the configuration. This configuration can contain the underlying Web Client configuration.
   */
  public SlimVaultClient(Vertx vertx, JsonObject configuration) {
    String host = configuration.getString("host");
    Integer port = configuration.getInteger("port", 8200);

    Objects.requireNonNull(host, "The Vault host must be set");

    client = WebClient.create(vertx, new WebClientOptions(configuration)
      .setDefaultPort(port).setDefaultHost(host)
    );

    setToken(configuration.getString("token"));
  }

  /**
   * Closes the client.
   */
  public void close() {
    if (client != null) {
      client.close();
    }
  }


  /**
   * Reads a secret from `path`.
   *
   * @param path            the path
   * @param responseHandler the callback invoked with the result
   */
  public void read(String path, Handler<AsyncResult<Secret>> responseHandler) {
    Objects.requireNonNull(responseHandler);

    client.get("/v1/" + Objects.requireNonNull(path))
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "No token to access the vault"))
      .send().onComplete(response -> {
        if (response.failed()) {
          responseHandler.handle(VaultException.toFailure("Unable to access the Vault", response.cause()));
          return;
        }

        HttpResponse<Buffer> result = response.result();
        if (result.statusCode() != 200) {
          responseHandler.handle(VaultException.toFailure(result.statusMessage(), result.statusCode(),
            result.bodyAsString()));
        } else {
          Secret secret = new Secret(result.bodyAsJsonObject());
          responseHandler.handle(Future.succeededFuture(secret));
        }
      });
  }

  /**
   * Write a secret to `path`.
   *
   * @param path          the path
   * @param resultHandler the callback invoked with the result
   */
  public void write(String path, JsonObject secrets, Handler<AsyncResult<Secret>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    client.post("/v1/" + Objects.requireNonNull(path))
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "The token must be set"))
      .sendJsonObject(Objects.requireNonNull(secrets, "The secret must be set")).onComplete(ar -> {
          if (ar.failed()) {
            resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
            return;
          }

          HttpResponse<Buffer> response = ar.result();
          switch (response.statusCode()) {
            case 200:
              resultHandler.handle(Future.succeededFuture(new Secret(response.bodyAsJsonObject())));
              break;
            case 204:
              resultHandler.handle(Future.succeededFuture());
              break;
            default:
              resultHandler.handle(VaultException.toFailure(response.statusMessage(), response.statusCode(),
                response.bodyAsString()));
          }
        });
  }

  /**
   * Lists secrets from path (children).
   *
   * @param path          the path
   * @param resultHandler the callback invoked with the result
   */
  public void list(String path, Handler<AsyncResult<List<String>>> resultHandler) {
    Objects.requireNonNull(path, "The path is required to list secrets");
    String fullPath = path + "?list=true";
    Objects.requireNonNull(resultHandler);

    read(fullPath, ar -> {
      if (ar.failed() && !(ar.cause() instanceof VaultException)) {
        resultHandler.handle(Future.failedFuture(ar.cause()));
      } else if (ar.failed()) {
        if (((VaultException) ar.cause()).getStatusCode() == 404) {
          resultHandler.handle(Future.succeededFuture(Collections.emptyList()));
        } else {
          resultHandler.handle(Future.failedFuture(ar.cause()));
        }
      } else {
        JsonArray keys = ar.result().getData().getJsonArray("keys");
        if (keys == null) {
          resultHandler.handle(Future.failedFuture("Cannot find keys"));
        } else {
          List<String> list = new ArrayList<>();
          keys.forEach(o -> list.add((String) o));
          resultHandler.handle(Future.succeededFuture(list));
        }
      }
    });
  }

  /**
   * Deletes a secret from `path`.
   *
   * @param path          the path
   * @param resultHandler the callback invoked with the result
   */
  public void delete(String path, Handler<AsyncResult<Void>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    client.delete("/v1/" + Objects.requireNonNull(path))
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "The token must be set"))
      .send().onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }

        HttpResponse<Buffer> response = ar.result();
        if (response.statusCode() != 204) {
          resultHandler.handle(VaultException.toFailure(response.statusMessage(), response.statusCode(),
            response.bodyAsString()));
        } else {
          resultHandler.handle(Future.succeededFuture());
        }
      });
  }

  /**
   * Creates a new token.
   *
   * @param tokenRequest  the token request
   * @param resultHandler the callback invoked with the result.
   */
  public void createToken(TokenRequest tokenRequest, Handler<AsyncResult<Auth>> resultHandler) {
    client.post("/v1/auth/token/create" + ((tokenRequest.getRole() == null) ? "" : "/" + tokenRequest.getRole()))
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "The token must be set"))
      .sendJsonObject(tokenRequest.toPayload()).onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }
        manageAuthResult(resultHandler, ar.result());
      });
  }

  private void manageAuthResult(Handler<AsyncResult<Auth>> resultHandler, HttpResponse<Buffer> response) {
    if (response.statusCode() != 200) {
      resultHandler.handle(VaultException.toFailure(response.statusMessage(), response.statusCode(),
        response.bodyAsString()));
    } else {
      JsonObject object = response.bodyAsJsonObject();
      resultHandler.handle(Future.succeededFuture(new Auth(object.getJsonObject("auth"))));
    }
  }

  /**
   * Logs in against the `AppRole` backend.
   *
   * @param roleId        the role id
   * @param secretId      the secret id
   * @param resultHandler the callback invoked with the result
   */
  public void loginWithAppRole(String roleId, String secretId, Handler<AsyncResult<Auth>>
    resultHandler) {
    JsonObject payload = new JsonObject()
      .put("role_id", Objects.requireNonNull(roleId, "The role must not be null"))
      .put("secret_id", Objects.requireNonNull(secretId, "The secret must not be null"));

    client.post("/v1/auth/approle/login")
      .sendJsonObject(payload).onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }

        manageAuthResult(resultHandler, ar.result());
      });
  }

  /**
   * Logs in against the `userpass` backend.
   *
   * @param username      the username
   * @param password      the password
   * @param resultHandler the callback invoked with the result
   */
  public void loginWithUserCredentials(String username, String password, Handler<AsyncResult<Auth>>
    resultHandler) {
    JsonObject payload = new JsonObject()
      .put("password", Objects.requireNonNull(password, "The password must not be null"));

    client.post("/v1/auth/userpass/login/" + Objects.requireNonNull(username, "The username must not be null"))
      .sendJsonObject(payload).onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }

        manageAuthResult(resultHandler, ar.result());
      });
  }

  /**
   * Logs in against the `Cert` backend. Certificates are configured directly on the client instance.
   *
   * @param resultHandler the callback invoked with the result
   */
  public void loginWithCert(Handler<AsyncResult<Auth>> resultHandler) {
    client.post("/v1/auth/cert/login")
      .send().onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }

        manageAuthResult(resultHandler, ar.result());
      });
  }

  /**
   * Renews the current token.
   *
   * @param leaseDurationInSecond the extension in second
   * @param resultHandler         the callback invoked with the result
   */
  public void renewSelf(long leaseDurationInSecond, Handler<AsyncResult<Auth>> resultHandler) {
    JsonObject payload = null;
    if (leaseDurationInSecond > 0) {
      payload = new JsonObject().put("increment", leaseDurationInSecond);
    }
    HttpRequest<Buffer> request = client.post("/v1/auth/token/renew-self")
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "The token must not be null"));

    Handler<AsyncResult<HttpResponse<Buffer>>> handler = ar -> {
      if (ar.failed()) {
        resultHandler.handle(VaultException.toFailure("Unable to access the Vault: " + ar.cause().getMessage(), ar.cause()));
        return;
      }
      manageAuthResult(resultHandler, ar.result());
    };

    if (payload != null) {
      request.sendJsonObject(payload).onComplete(handler);
    } else {
      request.send().onComplete(handler);
    }
  }

  /**
   * Looks up for the current token metadata.
   *
   * @param resultHandler the callback invoked with the result
   */
  public void lookupSelf(Handler<AsyncResult<Lookup>> resultHandler) {
    client.get("/v1/auth/token/lookup-self")
      .putHeader(TOKEN_HEADER, Objects.requireNonNull(getToken(), "The token must not be null"))
      .send().onComplete(ar -> {
        if (ar.failed()) {
          resultHandler.handle(VaultException.toFailure("Unable to access the Vault", ar.cause()));
          return;
        }
        HttpResponse<Buffer> response = ar.result();
        if (response.statusCode() != 200) {
          resultHandler.handle(VaultException.toFailure(response.statusMessage(), response.statusCode(),
            response.bodyAsString()));
        } else {
          JsonObject object = response.bodyAsJsonObject();
          JsonObject data = object.getJsonObject("data");
          if (data == null) {
            resultHandler.handle(Future.succeededFuture());
          } else {
            resultHandler.handle(Future.succeededFuture(new Lookup(data)));
          }
        }
      });
  }

  /**
   * @return the current token.
   */
  public synchronized String getToken() {
    return token;
  }

  /**
   * Sets the token.
   *
   * @param token the new token
   * @return the current {@link SlimVaultClient}
   */
  public synchronized SlimVaultClient setToken(String token) {
    this.token = token;
    return this;
  }
}
