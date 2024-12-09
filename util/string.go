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
package util

import (
	"math/rand"
	"strings"
	"time"
)

func RandString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	table := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		builder.WriteByte(table[r.Intn(62)])
	}
	return builder.String()
}

func GenerateNodeID() string {
	return RandString(40)
}

func IsUniqueSlice(list interface{}) bool {
	switch items := list.(type) {
	case []string:
		set := make(map[string]struct{})
		for _, item := range items {
			_, ok := set[item]
			if ok {
				return false
			}
			set[item] = struct{}{}
		}
		return true
	case []int:
		set := make(map[int]struct{})
		for _, item := range items {
			_, ok := set[item]
			if ok {
				return false
			}
			set[item] = struct{}{}
		}
		return true
	}

	panic("only support string and int")
}

func ContainsAny(target string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(target, sub) {
			return true
		}
	}
	return false
}
