package dbos

import (
	"context"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/stretchr/testify/require"
)

func TestListWorkflowsUsesStablePaginationOrder(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	workflow := func(Context, string) (string, error) { return "ok", nil }
	RegisterWorkflow(ctx, workflow)
	require.NoError(t, Launch(ctx))
	defer Shutdown(ctx, 10*time.Second)

	workflowIDs := []string{"workflow-c", "workflow-a", "workflow-d", "workflow-b"}
	for _, workflowID := range workflowIDs {
		handle, err := RunWorkflow(ctx, workflow, "", WithWorkflowID(workflowID))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)
	}

	systemDB := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := systemDB.RenderSQL(
		`UPDATE %sworkflow_status SET created_at = $1 WHERE workflow_uuid = $2`,
		systemDB.Dialect().SchemaPrefix(systemDB.Schema()),
	)
	for _, workflowID := range workflowIDs {
		_, err := systemDB.Pool().Exec(context.Background(), query, int64(1), workflowID)
		require.NoError(t, err)
	}

	limit := 2
	firstPage, err := systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
		WorkflowIDs: workflowIDs,
		Limit:       &limit,
	})
	require.NoError(t, err)
	offset := 2
	secondPage, err := systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
		WorkflowIDs: workflowIDs,
		Limit:       &limit,
		Offset:      &offset,
	})
	require.NoError(t, err)

	got := make([]string, 0, len(firstPage)+len(secondPage))
	for _, workflowStatus := range append(firstPage, secondPage...) {
		got = append(got, workflowStatus.ID)
	}
	require.Equal(t, []string{"workflow-a", "workflow-b", "workflow-c", "workflow-d"}, got,
		"pagination must use workflow UUID to break created_at ties")
}
