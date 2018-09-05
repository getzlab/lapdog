<template lang="html">
  <div id="workspace">
    <div class="modal" id="submission-modal">
      <div class="modal-content">
        <div class="container form-container">
          <h4>Execute Workflows</h4>
          <div class="row">
            <div class="input-field col s12">
              <select class="browser-default" id="config-select" v-model="submission_config" v-on:input="preflight(self_ref)">
                <option value="" selected disabled>Choose a method</option>
                <option v-for="config in method_configs" v-bind:value="config.name" :key="config.name">
                  {{config.name}}
                </option>
              </select>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s6 inline">
              <select class="browser-default" id="etype-select" v-model="submission_etype" v-on:input="preflight(self_ref)">
                <option value="" selected disabled>Choose an entity type</option>
                <option v-for="etype in entity_types" v-bind:value="etype.type" >
                  {{etype.type}}
                </option>
              </select>
            </div>
            <div class="input-field col s6">
              <input type="text" id="entity-input" required v-model="entity_field" v-on:input="preflight(self_ref)"/>
              <label style="z-index: -1;">Workflow Entity</label>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s12">
              <input type="text" id="expression-input" v-model="submission_expression" v-bind:disabled="extract_etype(submission_config) == submission_etype" v-on:input="preflight(self_ref)"/>
              <label style="z-index: -1;">Entity Expression (optional)</label>
            </div>
          </div>
          <div v-if="submission_message != ''" class="row">
            <div class="col s12" v-if="submission_message == '-'">
              Validating inputs...
            </div>
            <div v-else class="col s12" v-bind:class="submit_okay ? 'green-text' : 'red-text'">
              {{(submit_okay ? "" : "Error: ") + submission_message}}
            </div>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <a class="btn-flat" v-bind:class="submit_okay ? '' : 'disabled'" v-on:click="submit_workflow">Run</a>
      </div>
    </div>
    <h4>{{namespace}}/{{workspace}}</h4>
    <div class="divider">

    </div>
    <div class="row">
      <div class="col s6">
        <h5><a target="_blank" v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace">Visit Workspace in Firecloud</a></h5>
      </div>
      <div class="col s6">
        <h5>Lapdog Status</h5>
      </div>
    </div>
    <div class="row">
      <div class="col s12" v-if="!ws">
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div>
    </div>
    <div class="stats-container" v-if="ws">
      <div class="row">
        <div class="col s2">
          Google Bucket:
        </div>
        <div class="col s4">
          <a target="_blank" v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+ws.workspace.bucketName">
            {{ws.workspace.bucketName}}
          </a>
        </div>
        <div class="col s2">
          Execution Status:
        </div>
        <div class="col s4">
          <span v-if="!acl" class="orange-text text-darken-4">
            Pending...
          </span>
          <span v-else-if="acl.reason == 'permissions'" class="red-text">
            Insufficient permissions to check ACL
          </span>
          <span v-else-if="acl.failed" class="red-text">
            <span v-if="acl.reason == 'firecloud'">
              Firecloud API Error
            </span>
            <span v-else-if="acl.reason == 'acl-read'">
              Unable to parse workspace ACL
            </span>
          </span>
          <span v-else-if="acl.service_account" class="green-text">
            Ready to Execute
          </span>
          <span v-else-if="!acl.service_account">
            <a class='btn blue disabled'>Add Service Account</a>
          </span>
          <span v-else-if="acl_update && acl_update.reason=='success'" class="green-text">
            Ready to Execute
          </span>
          <span v-else-if="acl_update && acl_update.reason == 'gcloud'" class="red-text">
            Unable to access Gcloud Service Account
          </span>
          <span v-else-if="acl_update && acl_update.reason == 'firecloud'" class="red-text">
            Firecloud API Error
          </span>
          <span v-else-if="acl_update && acl_update.reason == 'acl-read'" class="red-text">
            Unable to parse workspace ACL
          </span>
          <span v-else-if="acl_update && acl_update.reason == 'permissions'" class="red-text">
            Insufficient permissions to add service account
          </span>
          <span v-else-if="acl_update.reason == 'account'">
            <a class="red-text" href="https://github.com/broadinstitute/firecloud-tools/tree/master/scripts/register_service_account">
              Register your service account with firecloud
            </a>
          </span>
          <span v-else class="red-text">
            Unknown error
          </span>
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Access Level:
        </div>
        <div class="col s4">
          {{ws.accessLevel}}
        </div>
        <div class="col s2">
          Workspace Cache State:
        </div>
        <div class="col s2">
          <span v-if="!cache_state" class="amber-text text-darken-4">
            Fetching...
          </span>
          <span v-else-if="cache_state == 'up-to-date'" class="green-text">
            Up to date
          </span>
          <span v-else-if="cache_state == 'sync'" class="cyan-text">
            Syncing...
          </span>
          <span v-else-if="cache_state == 'outdated'" class="amber-text text-darken-3">
            Out of date
          </span>
          <span v-else-if="cache_state == 'not-loaded'" class="red-text">
            Not Loaded
          </span>
        </div>
        <div class="col s2">
          <a class='btn blue' v-if="cache_state != 'up-to-date' && cache_state != 'sync'" v-on:click="sync_cache">Sync</a>
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Owner{{ws.owners.length > 1 ? 's' : ''}}:
        </div>
        <div class="col s4">
          {{ws.owners.join(', ')}}
        </div>
        <div class="col s6">
          <a href="#submission-modal" class='btn blue modal-trigger' >Execute new job</a>
        </div>
      </div>
      <div class="row">
        <div class="col s2">
          Active Submissions:
        </div>
        <div class="col s4">
          {{ws.workspaceSubmissionStats.runningSubmissionsCount}}
        </div>
        <div class="col s6">
          <a v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace+'/monitor'" class="btn blue">
            View your Firecloud Submissions
          </a>
        </div>
      </div>
    </div>
    <div class="submission-container">
      <div class="row">
        <div class="col s12">
          Labdog Submissions:
        </div>
      </div>
      <!-- <div v-if="submissions" class="collection">
        <router-link v-for="sub in submissions" class="collection-item black-text"
          :to="{name: 'submission', params: {namespace:sub.namespace, workspace:sub.workspace, submission_id:sub.submission_id}}">
          <div>
            Ran <strong>{{sub.methodConfigurationName}}</strong> on <strong>{{sub.submissionEntity.entityName}}</strong> ({{sub.submission_id}})
          </div>
        </router-link>
      </div>
      <div v-else>
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div> -->

      <div v-if="submissions" class="row">
        <div class="col s12">
          <table>
            <thead>
              <tr>
                <th>Configuration</th>
                <th>Entity</th>
                <th>Status</th>
                <th>Date</th>
                <th>Local Submission ID</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="sub in submissions">
                <td>{{sub.methodConfigurationName}}</td>
                <td>{{sub.submissionEntity.entityName}}</td>
                <td v-bind:class="sub.status == 'Failed' || sub.status == 'Error' ? 'red-text' : (sub.status == 'Succeeded' ? 'green-text' : '')">
                  {{sub.status}}
                </td>
                <td>{{sub.submissionDate}}</td>
                <td>
                  <router-link :to="{name: 'submission', params: {namespace:sub.namespace, workspace:sub.workspace, submission_id:sub.submission_id}}">
                    {{sub.submission_id}}
                  </router-link>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else>
        <div class="progress">
          <div class="indeterminate blue"></div>
        </div>
      </div>


    </div>
  </div>
</template>

<script>
  import axios from'axios'
  import _ from 'lodash'
  export default {
    props: ['namespace', 'workspace'],
    data() {
      return {
        ws: null,
        acl: null,
        acl_update: null,
        cache_state: null,
        method_configs:null,
        entity_types: null,
        entity_field: "",
        submission_config: "",
        submission_etype: "",
        submission_expression: "",
        submission_message: "",
        submissions: null,
        expr_disabled: false,
        submit_okay: false,
        // entities: null
      }
    },
    created() {
      this.getWorkspace(this.namespace, this.workspace);
      this.get_acl(this.namespace, this.workspace);
      // window.$('.modal').modal();
      // this.get_configs();
      // this.get_entities(this.namespace, this.workspace);
    },
    computed: {
      self_ref() {
        return this;
      }
    },
    methods: {
      preflight: _.debounce((_this) => {
        _this.submission_message = "Incomplete fields";
        if (_this.submission_config == "" || _this.submission_etype == "" || _this.entity_field == "") return;
        _this.submission_message = "-";
        console.log("Executing preflight");
        console.log(_this);
        let query = 'http://localhost:4201/api/v1/workspaces/'+_this.namespace+'/'+_this.workspace+"/preflight?";
        query += "config="+encodeURIComponent(_this.submission_config);
        query += "&entity="+encodeURIComponent(_this.entity_field);
        if (_this.submission_expression != "") query += "&expression="+encodeURIComponent(_this.submission_expression);
        if (_this.submission_etype != "") query += "&etype="+encodeURIComponent(_this.submission_etype);
        axios.post(query)
          .then(response => {
            console.log("Preflight returned");
            console.log(response);
            let result = response.data;
            _this.submit_okay = result.ok && !result.failed;
            if (_this.submit_okay) _this.submission_message = "Ready to submit " + result.workflows + " workflow(s)";
            else _this.submission_message = result.message;
          })
          .catch(response => {
            console.error("FAILED");
            console.error(response)
          })
      }, 1000),
      submit_workflow() {
        let query = 'http://localhost:4201/api/v1/workspaces/'+this.namespace+'/'+this.workspace+"/execute?";
        query += "config="+encodeURIComponent(this.submission_config);
        query += "&entity="+encodeURIComponent(this.entity_field);
        if (this.submission_expression != "") query += "&expression="+encodeURIComponent(this.submission_expression);
        if (this.submission_etype != "") query += "&etype="+encodeURIComponent(this.submission_etype);
        window.materialize.toast({
          html: "Preparing job...",
          displayLength: 5000,
        })
        axios.post(query)
          .then(response => {
            console.log("Execution returned");
            console.log(response);
            let result = response.data;
            if (result.ok && !result.failed) {
              window.materialize.toast({
                html: "It may take several minutes for the submission to check in",
                displayLength: 10000,
              })
              this.$router.push({
                name: 'submission',
                params: {
                  namespace: this.namespace,
                  workspace: this.workspace,
                  submission_id: response.data.local_id
                }
              });
            }
            else {
              alert("Execute failed: "+response.data.message);
            }
          })
          .catch(response => {
            console.error("FAILED");
            console.error(response)
          })
      },
      update_expr_mode() {
        this.expr_disabled = this.extract_etype(this.submission_config) == this.submission_etype;
        console.log(this.expr_disabled);
      },
      etype_crosscheck(cfg) {
        if(this.ws) {
          for(let config in this.ws.configs)
          {
            if(cfg == (config.namespace+'/'+config.name)) {
              console.log("Etype crosscheck: " +this.submission_etype == config.rootEntityType);
              return this.submission_etype == config.rootEntityType;
            }
          }
        }
        return false
      },
      extract_etype(cfg) {
        if(this.ws) {
          for(let config in this.ws.configs)
          {
            if(cfg == (this.ws.configs[config].name)) {
              return this.ws.configs[config].rootEntityType;
            }
          }
        }
        return false
      },
      validate_entity() {
        return this.entity_field == 'h' ? 'valid' : 'invalid';
      },
      getWorkspace(namespace, workspace) {
        axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace)
          .then(response => {
            console.log(response.data);
            this.ws = response.data;
            this.entity_types = response.data.entities;
            this.method_configs = response.data.configs;
            window.$('.modal').modal();
            axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/cache')
              .then(response => {
                console.log(response.data);
                this.cache_state = response.data;
                console.log("Fetching submissions");
                  axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/submissions')
                    .then(response => {
                      console.log("Fetched submissions");
                      console.log(response.data);
                      this.submissions = response.data;
                    })
                    .catch(error => {
                      console.error("FAIL");
                      console.error(error)
                    })
              })
              .catch(error => {
                console.error("FAIL");
                console.error(error)
              })
          })
          .catch(error => {
            console.error("FAIL");
            console.error(error)
          })
      },

     get_acl(namespace, workspace) {
        axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/acl')
          .then(response => {
            console.log("RESPONSE");
            console.log(response.data);
            this.acl = response.data
          })
          .catch(error => {
            console.error("FAIL")
            console.error(error)
          })
      },

      // get_entities(namespace, workspace) {
      //   axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/entities')
      //     .then(response => {
      //       this.entities = response.data.entity_types;
      //       for(let etype in this.entities.entity_types)
      //       {
      //         // this.get_cache_state(this.entities.entity_types[etype].type);
      //       }
      //     })
      //     .catch(error => {
      //       console.error("FAIL")
      //       console.error(response)
      //     })
      // },
      sync_cache() {
        this.cache_state = 'sync';
        axios.put('http://localhost:4201/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/cache')
          .then(response => {
            console.log(response.data);
            this.cache_state = response.data
          })
          .catch(error => {
            console.error("FAIL")
            console.error(response)
          })
      },
      get_configs() {
        axios.get('http://localhost:4201/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/configs')
          .then(response => {
            console.log(response.data);
            this.method_configs = response.data;
            window.$('.modal').modal();
            // this.$forceUpdate();
          })
          .catch(error => {
            console.error("FAIL");
            console.error(error)
          })
      }
      // get_cache_state(etype) {
      //   axios.get('http://localhost:4201/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/cache/'+etype)
      //     .then(response => {
      //       console.log("Fetched cache state for "+etype);
      //       this.cache_state[etype] = response.data;
      //       console.log(this.cache_state);
      //     })
      //     .catch(error => {
      //       console.error("FAIL")
      //       console.error(response)
      //     })
      // }
    },
    beforeRouteUpdate(to, from, next) {
      this.ws = null;
      this.acl = null;
      this.acl_update = null;
      this.entities = null;
      this.method_configs = null;
      this.submit_okay = false;
      this.submission_message = "";
      this.entity_field = "";
      this.submission_expression = "";
      this.submissions = null;
      this.getWorkspace(to.params.namespace, to.params.workspace);
      this.get_acl(to.params.namespace, to.params.workspace);
      // this.get_configs();
      next();
    }
  }
</script>
