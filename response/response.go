package response

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

type ErrorResponse struct {
	Msg    string `json:"msg"`
	Status int    `json:"-"`
}

type ErrorHandler struct {
	status map[error]ErrorResponse
}

func NewErrorHandler(errStatus map[error]ErrorResponse) ErrorHandler {
	status := map[error]ErrorResponse{
		pgx.ErrNoRows: {
			Msg:    "no rows found",
			Status: http.StatusNotFound,
		},
		fiber.ErrUnprocessableEntity: {
			Msg:    "validation error",
			Status: http.StatusUnprocessableEntity,
		},
	}

	for k, v := range errStatus {
		status[k] = v
	}

	return ErrorHandler{
		status: status,
	}
}

func (h ErrorHandler) Handler(ctx *fiber.Ctx, err error) error {
	e := h.getErrorResponse(err)
	log.Error().Err(err).Msg(e.Msg)
	return ctx.Status(e.Status).JSON(e)
}

func (h ErrorHandler) getErrorResponse(err error) ErrorResponse {
	var (
		ok bool
		e  ErrorResponse
	)

	if CheckPageNotFound(err) {
		return ErrorResponse{
			Msg:    "page not found",
			Status: http.StatusNotFound,
		}
	}

	if CheckDuplicateKey(err) {
		return ErrorResponse{
			Msg:    "duplicate key",
			Status: http.StatusBadRequest,
		}
	}

	if CheckValidationError(err) {
		return ErrorResponse{
			Msg:    err.Error(),
			Status: http.StatusUnprocessableEntity,
		}
	}

	e, ok = h.status[err]
	if !ok {
		e = ErrorResponse{
			Msg:    err.Error(),
			Status: http.StatusInternalServerError,
		}
	}
	if e.Msg == "" {
		e.Msg = err.Error()
	} else {
		e.Msg = fmt.Sprintf("%s %v", e.Msg, err)
	}

	return e
}

func CheckDuplicateKey(err error) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && pgError.Code == "23505"
}

func CheckPageNotFound(err error) bool {
	var fiberError *fiber.Error
	return errors.As(err, &fiberError) && fiberError.Code == http.StatusNotFound
}

func CheckValidationError(err error) bool {
	var validationErrors validator.ValidationErrors
	return errors.As(err, &validationErrors)
}
