// Jobcelis Terraform Provider
//
// This provider allows managing Jobcelis resources (webhooks, pipelines, jobs,
// event schemas, projects) as infrastructure-as-code using Terraform.
//
// Usage in Terraform:
//
//	terraform {
//	  required_providers {
//	    jobcelis = {
//	      source = "jobcelis/jobcelis"
//	    }
//	  }
//	}
//
//	provider "jobcelis" {
//	  api_key = var.jobcelis_api_key
//	  # auth_token = var.jobcelis_auth_token  # only for jobcelis_project
//	  # base_url = "https://jobcelis.com"  # optional
//	}
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: Provider,
	})
}

// Provider returns the Jobcelis Terraform provider.
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"api_key": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("JOBCELIS_API_KEY", nil),
				Description: "Project API key. Used by every resource and data source except jobcelis_project.",
			},
			"auth_token": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("JOBCELIS_AUTH_TOKEN", nil),
				Description: "User access token (JWT), sent as a Bearer token. Required only by the jobcelis_project resource and data source, whose endpoints do not accept an API key.",
			},
			"base_url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("JOBCELIS_BASE_URL", "https://jobcelis.com"),
				Description: "Base URL of the Jobcelis API.",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"jobcelis_webhook":      resourceWebhook(),
			"jobcelis_pipeline":     resourcePipeline(),
			"jobcelis_job":          resourceJob(),
			"jobcelis_event_schema": resourceEventSchema(),
			"jobcelis_project":      resourceProject(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"jobcelis_webhook":      dataSourceWebhook(),
			"jobcelis_pipeline":     dataSourcePipeline(),
			"jobcelis_job":          dataSourceJob(),
			"jobcelis_event_schema": dataSourceEventSchema(),
			"jobcelis_project":      dataSourceProject(),
		},
		ConfigureContextFunc: providerConfigure,
	}
}

// ---------------------------------------------------------------------------
// API Client
// ---------------------------------------------------------------------------

// apiClient performs authenticated HTTP requests against the Jobcelis API.
type apiClient struct {
	baseURL    string
	apiKey     string
	authToken  string
	httpClient *http.Client
}

func providerConfigure(_ context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	apiKey, _ := d.Get("api_key").(string)
	authToken, _ := d.Get("auth_token").(string)
	baseURL, _ := d.Get("base_url").(string)

	if apiKey == "" && authToken == "" {
		return nil, diag.Errorf("api_key (or JOBCELIS_API_KEY) is required; auth_token is needed only for jobcelis_project")
	}
	if baseURL == "" {
		baseURL = "https://jobcelis.com"
	}

	baseURL = strings.TrimRight(baseURL, "/")

	client := &apiClient{
		baseURL:   baseURL,
		apiKey:    apiKey,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	return client, nil
}

// doRequest executes an HTTP request authenticated with the project API key and
// returns the response body bytes.
// For DELETE requests that return 204, it returns nil bytes and no error.
// For 404 responses, it returns an empty body with no error (caller handles state removal).
func (c *apiClient) doRequest(method, path string, body interface{}) ([]byte, int, error) {
	if c.apiKey == "" {
		return nil, 0, fmt.Errorf("api_key is not set in the provider configuration")
	}
	return c.send(method, path, body, "X-Api-Key", c.apiKey)
}

// doUserRequest executes an HTTP request authenticated with the user access
// token (Bearer), for the endpoints that do not accept an API key (projects).
func (c *apiClient) doUserRequest(method, path string, body interface{}) ([]byte, int, error) {
	if c.authToken == "" {
		return nil, 0, fmt.Errorf("auth_token is not set in the provider configuration: the projects endpoints require a user access token, not an API key")
	}
	return c.send(method, path, body, "Authorization", "Bearer "+c.authToken)
}

func (c *apiClient) send(method, path string, body interface{}, authHeader, authValue string) ([]byte, int, error) {
	fullURL := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBytes)
	}

	req, err := http.NewRequest(method, fullURL, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, authValue)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request to %s %s failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.StatusCode, nil
	}

	if resp.StatusCode == http.StatusNoContent {
		return nil, resp.StatusCode, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf(
			"API %s %s returned status %d: %s",
			method, path, resp.StatusCode, string(respBody),
		)
	}

	return respBody, resp.StatusCode, nil
}

// decodeObject parses a JSON object response. Some endpoints wrap the object in
// {"data": {...}}; that envelope is removed.
func decodeObject(body []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if inner, ok := result["data"].(map[string]interface{}); ok {
		return inner, nil
	}
	return result, nil
}

// suppressEquivalentJSON hides diffs between two JSON strings that encode the
// same value (key order, whitespace).
func suppressEquivalentJSON(_, oldValue, newValue string, _ *schema.ResourceData) bool {
	if oldValue == newValue {
		return true
	}
	var o, n interface{}
	if err := json.Unmarshal([]byte(oldValue), &o); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(newValue), &n); err != nil {
		return false
	}
	ob, err1 := json.Marshal(o)
	nb, err2 := json.Marshal(n)
	return err1 == nil && err2 == nil && bytes.Equal(ob, nb)
}

// toInt converts a JSON number (float64) or numeric string to an int.
func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case string:
		i, err := strconv.Atoi(n)
		return i, err == nil
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Helper: convert []interface{} to []string for topic lists
// ---------------------------------------------------------------------------

func expandStringList(v []interface{}) []string {
	result := make([]string, len(v))
	for i, item := range v {
		result[i] = item.(string)
	}
	return result
}

// ---------------------------------------------------------------------------
// Resource: jobcelis_webhook
// ---------------------------------------------------------------------------

func resourceWebhook() *schema.Resource {
	return &schema.Resource{
		Description:   "Manages a Jobcelis webhook endpoint.",
		CreateContext: resourceWebhookCreate,
		ReadContext:   resourceWebhookRead,
		UpdateContext: resourceWebhookUpdate,
		DeleteContext: resourceWebhookDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"url": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The destination URL for webhook deliveries.",
			},
			"secret": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				Description: "Secret used for HMAC-SHA256 signature verification.",
			},
			"topics": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "List of event topic patterns to subscribe to (supports wildcards).",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Current status of the webhook (active/inactive).",
			},
		},
	}
}

func resourceWebhookCreate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"url": d.Get("url").(string),
	}
	if v, ok := d.GetOk("secret"); ok {
		payload["secret"] = v.(string)
	}
	if v, ok := d.GetOk("topics"); ok {
		payload["topics"] = expandStringList(v.([]interface{}))
	}

	body, _, err := client.doRequest("POST", "/api/v1/webhooks", payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create webhook: %w", err))
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse create webhook response: %w", err))
	}

	id, ok := result["id"].(string)
	if !ok {
		return diag.Errorf("API response missing 'id' field for created webhook")
	}
	d.SetId(id)

	return resourceWebhookRead(nil, d, m)
}

func resourceWebhookRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	body, statusCode, err := client.doRequest("GET", "/api/v1/webhooks/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read webhook %s: %w", d.Id(), err))
	}
	if statusCode == http.StatusNotFound {
		d.SetId("")
		return nil
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse webhook response: %w", err))
	}

	if v, ok := result["url"]; ok {
		_ = d.Set("url", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["topics"]; ok {
		_ = d.Set("topics", v)
	}

	return nil
}

func resourceWebhookUpdate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"url": d.Get("url").(string),
	}
	if d.HasChange("secret") {
		payload["secret"] = d.Get("secret").(string)
	}
	if v, ok := d.GetOk("topics"); ok {
		payload["topics"] = expandStringList(v.([]interface{}))
	}

	_, _, err := client.doRequest("PATCH", "/api/v1/webhooks/"+url.PathEscape(d.Id()), payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to update webhook %s: %w", d.Id(), err))
	}

	return resourceWebhookRead(nil, d, m)
}

func resourceWebhookDelete(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	_, _, err := client.doRequest("DELETE", "/api/v1/webhooks/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete webhook %s: %w", d.Id(), err))
	}

	d.SetId("")
	return nil
}

// ---------------------------------------------------------------------------
// Resource: jobcelis_pipeline
// ---------------------------------------------------------------------------

func resourcePipeline() *schema.Resource {
	return &schema.Resource{
		Description:   "Manages a Jobcelis event pipeline.",
		CreateContext: resourcePipelineCreate,
		ReadContext:   resourcePipelineRead,
		UpdateContext: resourcePipelineUpdate,
		DeleteContext: resourcePipelineDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the pipeline.",
			},
			"description": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Description of what this pipeline does.",
			},
			"webhook_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "ID of the target webhook for delivery.",
			},
			"topics": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Event topic patterns that trigger this pipeline.",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
			"steps": {
				Type:             schema.TypeString,
				Optional:         true,
				Description:      "JSON-encoded array of pipeline steps.",
				DiffSuppressFunc: suppressEquivalentJSON,
			},
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Current status (active/inactive).",
			},
		},
	}
}

func resourcePipelineCreate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name":       d.Get("name").(string),
		"webhook_id": d.Get("webhook_id").(string),
	}
	if v, ok := d.GetOk("description"); ok {
		payload["description"] = v.(string)
	}
	if v, ok := d.GetOk("topics"); ok {
		payload["topics"] = expandStringList(v.([]interface{}))
	}
	if v, ok := d.GetOk("steps"); ok {
		// Send steps as parsed JSON, not a string
		var steps interface{}
		if err := json.Unmarshal([]byte(v.(string)), &steps); err != nil {
			return diag.FromErr(fmt.Errorf("'steps' must be valid JSON: %w", err))
		}
		payload["steps"] = steps
	}

	body, _, err := client.doRequest("POST", "/api/v1/pipelines", payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create pipeline: %w", err))
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse create pipeline response: %w", err))
	}

	id, ok := result["id"].(string)
	if !ok {
		return diag.Errorf("API response missing 'id' field for created pipeline")
	}
	d.SetId(id)

	return resourcePipelineRead(nil, d, m)
}

func resourcePipelineRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	body, statusCode, err := client.doRequest("GET", "/api/v1/pipelines/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read pipeline %s: %w", d.Id(), err))
	}
	if statusCode == http.StatusNotFound {
		d.SetId("")
		return nil
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse pipeline response: %w", err))
	}

	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}
	if v, ok := result["description"]; ok {
		_ = d.Set("description", v)
	}
	if v, ok := result["webhook_id"]; ok {
		_ = d.Set("webhook_id", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["topics"]; ok {
		_ = d.Set("topics", v)
	}
	if v, ok := result["steps"]; ok {
		stepsJSON, err := json.Marshal(v)
		if err == nil {
			_ = d.Set("steps", string(stepsJSON))
		}
	}

	return nil
}

func resourcePipelineUpdate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name":       d.Get("name").(string),
		"webhook_id": d.Get("webhook_id").(string),
	}
	if v, ok := d.GetOk("description"); ok {
		payload["description"] = v.(string)
	}
	if v, ok := d.GetOk("topics"); ok {
		payload["topics"] = expandStringList(v.([]interface{}))
	}
	if v, ok := d.GetOk("steps"); ok {
		var steps interface{}
		if err := json.Unmarshal([]byte(v.(string)), &steps); err != nil {
			return diag.FromErr(fmt.Errorf("'steps' must be valid JSON: %w", err))
		}
		payload["steps"] = steps
	}

	_, _, err := client.doRequest("PATCH", "/api/v1/pipelines/"+url.PathEscape(d.Id()), payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to update pipeline %s: %w", d.Id(), err))
	}

	return resourcePipelineRead(nil, d, m)
}

func resourcePipelineDelete(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	_, _, err := client.doRequest("DELETE", "/api/v1/pipelines/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete pipeline %s: %w", d.Id(), err))
	}

	d.SetId("")
	return nil
}

// ---------------------------------------------------------------------------
// Resource: jobcelis_job
// ---------------------------------------------------------------------------

func resourceJob() *schema.Resource {
	return &schema.Resource{
		Description:   "Manages a Jobcelis scheduled job.",
		CreateContext: resourceJobCreate,
		ReadContext:   resourceJobRead,
		UpdateContext: resourceJobUpdate,
		DeleteContext: resourceJobDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the scheduled job.",
			},
			"queue": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Queue the job runs on (e.g. delivery, scheduled_job, default).",
			},
			"cron_expression": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Cron expression defining the job schedule.",
			},
			"topics": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Event topics for the job. The API emits the job's event on the first topic.",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
			"webhook_id": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "ID of the webhook to deliver results to.",
			},
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Current status of the job.",
			},
		},
	}
}

func resourceJobCreate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name":            d.Get("name").(string),
		"queue":           d.Get("queue").(string),
		"cron_expression": d.Get("cron_expression").(string),
	}
	if v, ok := d.GetOk("topics"); ok {
		topics := expandStringList(v.([]interface{}))
		payload["topics"] = topics
		// The API runs a job's action on a single topic.
		if len(topics) > 0 {
			payload["topic"] = topics[0]
		}
	}
	if v, ok := d.GetOk("webhook_id"); ok {
		payload["webhook_id"] = v.(string)
	}

	body, _, err := client.doRequest("POST", "/api/v1/jobs", payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create job: %w", err))
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse create job response: %w", err))
	}

	id, ok := result["id"].(string)
	if !ok {
		return diag.Errorf("API response missing 'id' field for created job")
	}
	d.SetId(id)

	return resourceJobRead(nil, d, m)
}

func resourceJobRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	body, statusCode, err := client.doRequest("GET", "/api/v1/jobs/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read job %s: %w", d.Id(), err))
	}
	if statusCode == http.StatusNotFound {
		d.SetId("")
		return nil
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse job response: %w", err))
	}

	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}
	if v, ok := result["queue"]; ok {
		_ = d.Set("queue", v)
	}
	if v, ok := result["cron_expression"]; ok {
		_ = d.Set("cron_expression", v)
	}
	if v, ok := result["topics"]; ok {
		_ = d.Set("topics", v)
	}
	if v, ok := result["webhook_id"]; ok {
		_ = d.Set("webhook_id", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}

	return nil
}

func resourceJobUpdate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name":            d.Get("name").(string),
		"queue":           d.Get("queue").(string),
		"cron_expression": d.Get("cron_expression").(string),
	}
	if v, ok := d.GetOk("topics"); ok {
		topics := expandStringList(v.([]interface{}))
		payload["topics"] = topics
		// The API runs a job's action on a single topic.
		if len(topics) > 0 {
			payload["topic"] = topics[0]
		}
	}
	if v, ok := d.GetOk("webhook_id"); ok {
		payload["webhook_id"] = v.(string)
	}

	_, _, err := client.doRequest("PATCH", "/api/v1/jobs/"+url.PathEscape(d.Id()), payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to update job %s: %w", d.Id(), err))
	}

	return resourceJobRead(nil, d, m)
}

func resourceJobDelete(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	_, _, err := client.doRequest("DELETE", "/api/v1/jobs/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete job %s: %w", d.Id(), err))
	}

	d.SetId("")
	return nil
}

// ---------------------------------------------------------------------------
// Resource: jobcelis_event_schema
// ---------------------------------------------------------------------------

func resourceEventSchema() *schema.Resource {
	return &schema.Resource{
		Description:   "Manages a Jobcelis event schema for topic validation.",
		CreateContext: resourceEventSchemaCreate,
		ReadContext:   resourceEventSchemaRead,
		UpdateContext: resourceEventSchemaUpdate,
		DeleteContext: resourceEventSchemaDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"topic": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The event topic this schema applies to.",
			},
			"version": {
				Type:        schema.TypeInt,
				Optional:    true,
				Computed:    true,
				Description: "Schema version (an integer; the API defaults to 1).",
			},
			"schema_body": {
				Type:             schema.TypeString,
				Required:         true,
				Description:      "JSON string defining the event schema (sent to the API as \"schema\").",
				DiffSuppressFunc: suppressEquivalentJSON,
			},
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Current status of the schema.",
			},
		},
	}
}

func resourceEventSchemaCreate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	// Parse schema_body to send as JSON object, not a string
	var schemaBody interface{}
	if err := json.Unmarshal([]byte(d.Get("schema_body").(string)), &schemaBody); err != nil {
		return diag.FromErr(fmt.Errorf("'schema_body' must be valid JSON: %w", err))
	}

	payload := map[string]interface{}{
		"topic":  d.Get("topic").(string),
		"schema": schemaBody,
	}
	if v, ok := d.GetOk("version"); ok {
		payload["version"] = v.(int)
	}

	body, _, err := client.doRequest("POST", "/api/v1/event-schemas", payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create event schema: %w", err))
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse create event schema response: %w", err))
	}

	id, ok := result["id"].(string)
	if !ok {
		return diag.Errorf("API response missing 'id' field for created event schema")
	}
	d.SetId(id)

	return resourceEventSchemaRead(nil, d, m)
}

func resourceEventSchemaRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	body, statusCode, err := client.doRequest("GET", "/api/v1/event-schemas/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read event schema %s: %w", d.Id(), err))
	}
	if statusCode == http.StatusNotFound {
		d.SetId("")
		return nil
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse event schema response: %w", err))
	}

	if v, ok := result["topic"]; ok {
		_ = d.Set("topic", v)
	}
	if v, ok := toInt(result["version"]); ok {
		_ = d.Set("version", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["schema"]; ok {
		schemaJSON, err := json.Marshal(v)
		if err == nil {
			_ = d.Set("schema_body", string(schemaJSON))
		}
	}

	return nil
}

func resourceEventSchemaUpdate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	var schemaBody interface{}
	if err := json.Unmarshal([]byte(d.Get("schema_body").(string)), &schemaBody); err != nil {
		return diag.FromErr(fmt.Errorf("'schema_body' must be valid JSON: %w", err))
	}

	payload := map[string]interface{}{
		"topic":  d.Get("topic").(string),
		"schema": schemaBody,
	}
	if v, ok := d.GetOk("version"); ok {
		payload["version"] = v.(int)
	}

	_, _, err := client.doRequest("PATCH", "/api/v1/event-schemas/"+url.PathEscape(d.Id()), payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to update event schema %s: %w", d.Id(), err))
	}

	return resourceEventSchemaRead(nil, d, m)
}

func resourceEventSchemaDelete(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	_, _, err := client.doRequest("DELETE", "/api/v1/event-schemas/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete event schema %s: %w", d.Id(), err))
	}

	d.SetId("")
	return nil
}

// ---------------------------------------------------------------------------
// Resource: jobcelis_project
// ---------------------------------------------------------------------------

func resourceProject() *schema.Resource {
	return &schema.Resource{
		Description:   "Manages a Jobcelis project. Requires auth_token (a user access token) in the provider configuration: the projects endpoints do not accept an API key.",
		CreateContext: resourceProjectCreate,
		ReadContext:   resourceProjectRead,
		UpdateContext: resourceProjectUpdate,
		DeleteContext: resourceProjectDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of the project.",
			},
		},
	}
}

func resourceProjectCreate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name": d.Get("name").(string),
	}

	body, _, err := client.doUserRequest("POST", "/api/v1/projects", payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create project: %w", err))
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse create project response: %w", err))
	}

	id, ok := result["id"].(string)
	if !ok {
		return diag.Errorf("API response missing 'id' field for created project")
	}
	d.SetId(id)

	return resourceProjectRead(nil, d, m)
}

func resourceProjectRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	body, statusCode, err := client.doUserRequest("GET", "/api/v1/projects/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read project %s: %w", d.Id(), err))
	}
	if statusCode == http.StatusNotFound {
		d.SetId("")
		return nil
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse project response: %w", err))
	}

	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}

	return nil
}

func resourceProjectUpdate(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	payload := map[string]interface{}{
		"name": d.Get("name").(string),
	}

	_, _, err := client.doUserRequest("PATCH", "/api/v1/projects/"+url.PathEscape(d.Id()), payload)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to update project %s: %w", d.Id(), err))
	}

	return resourceProjectRead(nil, d, m)
}

func resourceProjectDelete(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)

	_, _, err := client.doUserRequest("DELETE", "/api/v1/projects/"+url.PathEscape(d.Id()), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete project %s: %w", d.Id(), err))
	}

	d.SetId("")
	return nil
}

// ---------------------------------------------------------------------------
// Data Source: jobcelis_webhook
// ---------------------------------------------------------------------------

func dataSourceWebhook() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about an existing Jobcelis webhook.",
		ReadContext: dataSourceWebhookRead,
		Schema: map[string]*schema.Schema{
			"webhook_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID of the webhook to look up.",
			},
			"url":    {Type: schema.TypeString, Computed: true, Description: "The destination URL."},
			"status": {Type: schema.TypeString, Computed: true, Description: "Current status."},
			"topics": {Type: schema.TypeList, Computed: true, Elem: &schema.Schema{Type: schema.TypeString}, Description: "Subscribed topics."},
		},
	}
}

func dataSourceWebhookRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)
	webhookID := d.Get("webhook_id").(string)

	body, statusCode, err := client.doRequest("GET", "/api/v1/webhooks/"+url.PathEscape(webhookID), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read webhook %s: %w", webhookID, err))
	}
	if statusCode == http.StatusNotFound {
		return diag.Errorf("webhook %s not found", webhookID)
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse webhook response: %w", err))
	}

	d.SetId(webhookID)
	if v, ok := result["url"]; ok {
		_ = d.Set("url", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["topics"]; ok {
		_ = d.Set("topics", v)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Data Source: jobcelis_pipeline
// ---------------------------------------------------------------------------

func dataSourcePipeline() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about an existing Jobcelis pipeline.",
		ReadContext: dataSourcePipelineRead,
		Schema: map[string]*schema.Schema{
			"pipeline_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID of the pipeline to look up.",
			},
			"name":       {Type: schema.TypeString, Computed: true, Description: "Pipeline name."},
			"status":     {Type: schema.TypeString, Computed: true, Description: "Current status."},
			"webhook_id": {Type: schema.TypeString, Computed: true, Description: "Associated webhook ID."},
		},
	}
}

func dataSourcePipelineRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)
	pipelineID := d.Get("pipeline_id").(string)

	body, statusCode, err := client.doRequest("GET", "/api/v1/pipelines/"+url.PathEscape(pipelineID), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read pipeline %s: %w", pipelineID, err))
	}
	if statusCode == http.StatusNotFound {
		return diag.Errorf("pipeline %s not found", pipelineID)
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse pipeline response: %w", err))
	}

	d.SetId(pipelineID)
	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["webhook_id"]; ok {
		_ = d.Set("webhook_id", v)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Data Source: jobcelis_job
// ---------------------------------------------------------------------------

func dataSourceJob() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about an existing Jobcelis scheduled job.",
		ReadContext: dataSourceJobRead,
		Schema: map[string]*schema.Schema{
			"job_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID of the job to look up.",
			},
			"name":            {Type: schema.TypeString, Computed: true, Description: "Job name."},
			"queue":           {Type: schema.TypeString, Computed: true, Description: "Queue name."},
			"cron_expression": {Type: schema.TypeString, Computed: true, Description: "Cron schedule."},
			"webhook_id":      {Type: schema.TypeString, Computed: true, Description: "Associated webhook ID."},
			"status":          {Type: schema.TypeString, Computed: true, Description: "Current status."},
			"topics":          {Type: schema.TypeList, Computed: true, Elem: &schema.Schema{Type: schema.TypeString}, Description: "Event topics."},
		},
	}
}

func dataSourceJobRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)
	jobID := d.Get("job_id").(string)

	body, statusCode, err := client.doRequest("GET", "/api/v1/jobs/"+url.PathEscape(jobID), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read job %s: %w", jobID, err))
	}
	if statusCode == http.StatusNotFound {
		return diag.Errorf("job %s not found", jobID)
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse job response: %w", err))
	}

	d.SetId(jobID)
	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}
	if v, ok := result["queue"]; ok {
		_ = d.Set("queue", v)
	}
	if v, ok := result["cron_expression"]; ok {
		_ = d.Set("cron_expression", v)
	}
	if v, ok := result["webhook_id"]; ok {
		_ = d.Set("webhook_id", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["topics"]; ok {
		_ = d.Set("topics", v)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Data Source: jobcelis_event_schema
// ---------------------------------------------------------------------------

func dataSourceEventSchema() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about an existing Jobcelis event schema.",
		ReadContext: dataSourceEventSchemaRead,
		Schema: map[string]*schema.Schema{
			"event_schema_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID of the event schema to look up.",
			},
			"topic":       {Type: schema.TypeString, Computed: true, Description: "Event topic."},
			"version":     {Type: schema.TypeInt, Computed: true, Description: "Schema version."},
			"schema_body": {Type: schema.TypeString, Computed: true, Description: "JSON schema body."},
			"status":      {Type: schema.TypeString, Computed: true, Description: "Current status."},
		},
	}
}

func dataSourceEventSchemaRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)
	schemaID := d.Get("event_schema_id").(string)

	body, statusCode, err := client.doRequest("GET", "/api/v1/event-schemas/"+url.PathEscape(schemaID), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read event schema %s: %w", schemaID, err))
	}
	if statusCode == http.StatusNotFound {
		return diag.Errorf("event schema %s not found", schemaID)
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse event schema response: %w", err))
	}

	d.SetId(schemaID)
	if v, ok := result["topic"]; ok {
		_ = d.Set("topic", v)
	}
	if v, ok := toInt(result["version"]); ok {
		_ = d.Set("version", v)
	}
	if v, ok := result["status"]; ok {
		_ = d.Set("status", v)
	}
	if v, ok := result["schema"]; ok {
		schemaJSON, err := json.Marshal(v)
		if err == nil {
			_ = d.Set("schema_body", string(schemaJSON))
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Data Source: jobcelis_project
// ---------------------------------------------------------------------------

func dataSourceProject() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about an existing Jobcelis project. Requires auth_token in the provider configuration.",
		ReadContext: dataSourceProjectRead,
		Schema: map[string]*schema.Schema{
			"project_id": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The ID of the project to look up.",
			},
			"name": {Type: schema.TypeString, Computed: true, Description: "Project name."},
		},
	}
}

func dataSourceProjectRead(_ context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*apiClient)
	projectID := d.Get("project_id").(string)

	body, statusCode, err := client.doUserRequest("GET", "/api/v1/projects/"+url.PathEscape(projectID), nil)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to read project %s: %w", projectID, err))
	}
	if statusCode == http.StatusNotFound {
		return diag.Errorf("project %s not found", projectID)
	}

	result, err := decodeObject(body)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to parse project response: %w", err))
	}

	d.SetId(projectID)
	if v, ok := result["name"]; ok {
		_ = d.Set("name", v)
	}

	return nil
}
