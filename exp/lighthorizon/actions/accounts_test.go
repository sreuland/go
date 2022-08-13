package actions

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/exp/lighthorizon/common"
	"github.com/stellar/go/exp/lighthorizon/services"
	"github.com/stellar/go/support/render/problem"
)

func TestTxByAccountMissingParamError(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := makeRequest(
		t,
		map[string]string{},
		map[string]string{},
	)

	mockOperationService := &services.MockOperationService{}
	mockTransactionService := &services.MockTransactionService{}

	lh := services.LightHorizon{
		Operations:   mockOperationService,
		Transactions: mockTransactionService,
	}

	handler := NewTXByAccountHandler(lh)
	handler(recorder, request)

	resp := recorder.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	raw, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var problem problem.P
	err = json.Unmarshal(raw, &problem)
	assert.NoError(t, err)
	assert.Equal(t, "Bad Request", problem.Title)
	assert.Equal(t, "invalid request parameters", problem.Extras["invalid_field"])
	assert.Equal(t, "unable to find account_id in url path", problem.Extras["reason"])
}

func TestTxByAccountServerError(t *testing.T) {
	recorder := httptest.NewRecorder()
	pathParams := make(map[string]string)
	pathParams["account_id"] = "G1234"
	request := makeRequest(
		t,
		map[string]string{},
		pathParams,
	)

	mockOperationService := &services.MockOperationService{}
	mockTransactionService := &services.MockTransactionService{}
	mockTransactionService.On("GetTransactionsByAccount", mock.Anything, mock.Anything, mock.Anything, "G1234").Return([]common.Transaction{}, errors.New("not good"))

	lh := services.LightHorizon{
		Operations:   mockOperationService,
		Transactions: mockTransactionService,
	}

	handler := NewTXByAccountHandler(lh)
	handler(recorder, request)

	resp := recorder.Result()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	raw, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var problem problem.P
	err = json.Unmarshal(raw, &problem)
	assert.NoError(t, err)
	assert.Equal(t, "Internal Server Error", problem.Title)
}

func TestOpsByAccountMissingParamError(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := makeRequest(
		t,
		map[string]string{},
		map[string]string{},
	)

	mockOperationService := &services.MockOperationService{}
	mockTransactionService := &services.MockTransactionService{}

	lh := services.LightHorizon{
		Operations:   mockOperationService,
		Transactions: mockTransactionService,
	}

	handler := NewOpsByAccountHandler(lh)
	handler(recorder, request)

	resp := recorder.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	raw, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var problem problem.P
	err = json.Unmarshal(raw, &problem)
	assert.NoError(t, err)
	assert.Equal(t, "Bad Request", problem.Title)
	assert.Equal(t, "invalid request parameters", problem.Extras["invalid_field"])
	assert.Equal(t, "unable to find account_id in url path", problem.Extras["reason"])
}

func TestOpsByAccountServerError(t *testing.T) {
	recorder := httptest.NewRecorder()
	pathParams := make(map[string]string)
	pathParams["account_id"] = "G1234"
	request := makeRequest(
		t,
		map[string]string{},
		pathParams,
	)

	mockOperationService := &services.MockOperationService{}
	mockTransactionService := &services.MockTransactionService{}
	mockOperationService.On("GetOperationsByAccount", mock.Anything, mock.Anything, mock.Anything, "G1234").Return([]common.Operation{}, errors.New("not good"))

	lh := services.LightHorizon{
		Operations:   mockOperationService,
		Transactions: mockTransactionService,
	}

	handler := NewOpsByAccountHandler(lh)
	handler(recorder, request)

	resp := recorder.Result()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	raw, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var problem problem.P
	err = json.Unmarshal(raw, &problem)
	assert.NoError(t, err)
	assert.Equal(t, "Internal Server Error", problem.Title)
}
