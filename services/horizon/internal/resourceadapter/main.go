package resourceadapter

type ResourceAdapter struct {
	SkipTxmeta bool
}

var resourceAdapterInstance ResourceAdapter

// provide a singleton reference to resource adapter runtime state
func GetResourceAdapter() ResourceAdapter {
	return resourceAdapterInstance
}

func SetResourceAdapter(instance ResourceAdapter) {
	resourceAdapterInstance = instance
}
