// Copyright 2026 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import "fmt"

// EncodePrepareV1 encodes a Prepare request that asks the server to include
// the byte offset of the first prepared statement in its response.
func EncodePrepareV1(request *Message, db uint64, sql string) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)
	request.putHeader(RequestPrepare, 1)
}

// DecodeStmtWithOffset decodes a version 1 Stmt response.
func DecodeStmtWithOffset(response *Message) (db uint32, id uint32, params uint64, offset uint64, err error) {
	mtype, _ := response.getHeader()

	if mtype == ResponseFailure {
		e := ErrRequest{}
		e.Code = response.getUint64()
		e.Description = response.getString()
		err = e
		return
	}

	if mtype != ResponseStmt {
		err = fmt.Errorf("decode %s: unexpected type %d", responseDesc(ResponseStmt), mtype)
		return
	}

	db = response.getUint32()
	id = response.getUint32()
	params = response.getUint64()
	offset = response.getUint64()
	return
}
