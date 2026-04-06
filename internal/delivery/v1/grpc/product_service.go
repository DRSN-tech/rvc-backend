package grpc

import (
	"context"

	"github.com/DRSN-tech/go-backend/internal/proto"
	"github.com/DRSN-tech/go-backend/internal/usecase"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
)

type ProductService struct {
	proto.UnimplementedProductServiceServer
	prUC   usecase.ProductUC
	logger logger.Logger
}

func NewProductService(prUC usecase.ProductUC, logger logger.Logger) *ProductService {
	return &ProductService{prUC: prUC, logger: logger}
}

func (g *ProductService) GetProductsInfo(ctx context.Context, req *proto.ProductsInfoRequest) (*proto.ProductsInfoResponse, error) {
	const op = "grpc.GetProductsInfo"

	seen := make(map[int64]struct{}, len(req.Ids))
	uniqueProducts := make([]int64, 0, len(req.Ids))
	for _, id := range req.Ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniqueProducts = append(uniqueProducts, id)
	}

	res, err := g.prUC.GetProductsInfo(ctx, usecase.NewGetProductsReq(uniqueProducts))
	if err != nil {
		g.logger.Errorf(e.Wrap(op, err), "%s", op)
		return nil, GRPCErrorResponse(e.Wrap(op, err))
	}

	return &proto.ProductsInfoResponse{
		Products:         toArrGRPCProduct(res.Products),
		ProductsNotFound: res.NotFoundProducts,
	}, nil
}

func toGRPCProduct(pr *usecase.ProductInfo) *proto.Product {
	return &proto.Product{
		Id:       pr.ID,
		Name:     pr.Name,
		Category: pr.CategoryName,
		Price:    pr.Price,
	}
}

func toArrGRPCProduct(prs []usecase.ProductInfo) []*proto.Product {
	res := make([]*proto.Product, len(prs))
	for i, p := range prs {
		res[i] = toGRPCProduct(&p)
	}

	return res
}
