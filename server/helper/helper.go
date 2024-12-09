/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/probe"
	"github.com/apache/kvrocks-controller/util"
)

// Error Message
type Error struct {
	Message string `json:"message"`
}

// Response General Response Structure
// @Description General Response Structure
type Response struct {
	Error *Error      `json:"error,omitempty"`
	Data  interface{} `json:"data"`
}

func ResponseUnmarshal[T any](r io.Reader) (*T, error) {
	// todo: replace the original `Response` struct with the new one
	type dummyResponse struct {
		Error *Error `json:"error,omitempty"`
		Data  T      `json:"data"`
	}

	var resp dummyResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, errors.New(resp.Error.Message)
	}

	return &resp.Data, nil
}

func ResponseOK(c *gin.Context, data interface{}) {
	responseData(c, http.StatusOK, data)
}

func ResponseCreated(c *gin.Context, data interface{}) {
	responseData(c, http.StatusCreated, data)
}

func ResponseNoContent(c *gin.Context) {
	c.JSON(http.StatusNoContent, nil)
}

func ResponseBadRequest(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, Response{
		Error: &Error{Message: err.Error()},
	})
}

func responseData(c *gin.Context, code int, data interface{}) {
	c.JSON(code, Response{
		Data: data,
	})
}

func ResponseError(c *gin.Context, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, consts.ErrNotFound) {
		code = http.StatusNotFound
	} else if errors.Is(err, consts.ErrIndexOutOfRange) {
		code = http.StatusBadRequest
	} else if errors.Is(err, consts.ErrAlreadyExists) {
		code = http.StatusConflict
	} else if errors.Is(err, consts.ErrForbidden) {
		code = http.StatusForbidden
	} else if errors.Is(err, consts.ErrInvalidArgument) {
		code = http.StatusBadRequest
	}
	c.JSON(code, Response{
		Error: &Error{Message: err.Error()},
	})
	c.Abort()
}

// generateSessionID encodes the addr to a session ID,
// which is used to identify the session. And then can be used to
// parse the leader listening address back.
func GenerateSessionID(addr string) string {
	return fmt.Sprintf("%s/%s", util.RandString(8), addr)
}

// extractAddrFromSessionID decodes the session ID to the addr.
func ExtractAddrFromSessionID(sessionID string) string {
	parts := strings.Split(sessionID, "/")
	if len(parts) != 2 {
		// for the old session ID format, we use the addr as the session ID
		return sessionID
	}
	return parts[1]
}

func HealthQueryUrl(addr, namespace, cluster, node string) string {
	return fmt.Sprintf("http://%s/api/v1/namespaces/%s/clusters/%s/nodes/%s/health", addr, namespace, cluster, node)
}

func QueryHealthStatus(ctx context.Context, addr, namespace, cluster, node string) (*probe.ProbeStatus, error) {
	url := HealthQueryUrl(addr, namespace, cluster, node)

	httpClient := http.Client{}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status code: %d", resp.StatusCode)
	}

	return ResponseUnmarshal[probe.ProbeStatus](resp.Body)
}
