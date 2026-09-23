package integration

import (
	"grantline/internal/engine"
	"grantline/internal/target/pgtarget"
)

func small(c *engine.Config, _ *pgtarget.Config) { c.ChunkRows, c.Copiers = 500, 3 }

type pgtargetConfig = pgtarget.Config
