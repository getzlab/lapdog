<template lang="html">
  <div id="workspace">
    <!-- <div class="modal" id="submission-modal">
      <div class="modal-content">
        <div class="container form-container">
          <h4>Execute Workflows</h4>
          <div class="row">
            <div class="input-field col s12">
              <select class="browser-default" id="config-select">
                <option value="" selected disabled>Choose a method</option>
                <option v-for="config in method_configs" v-bind:value="config.namespace+'/'+config.name">
                  config.name
                </option>
              </select>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s6">
              <select class="browser-default" id="config-select">
                <option value="" selected disabled>Choose an entity type</option>
                <option v-for="etype in entity_types" v-bind:value="etype">
                  etype
                </option>
              </select>
            </div>
            <div class="input-field col s6">
              <input type="text" id="entity-input" required/>
              <label>Workflow Entity</label>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s12">
              <input type="text" id="expression-input"/>
              <label>Entity Expression (optional)</label>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <a class="btn-flat" v-on:click="submit_workflow">Run</a>
      </div>
    </div> -->
    <h4>{{namespace}}/{{workspace}}</h4>
    <div class="divider">

    </div>
    <div class="row">
      <div class="col s6">
        <h5><a target="_blank" v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace">Workspace Info</a></h5>
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
          Workspace Cache State
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
      </div>
      <div class="row">
        <div class="col s2">
          Active Submissions:
        </div>
        <div class="col s4">
          {{ws.workspaceSubmissionStats.runningSubmissionsCount}}
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  import axios from'axios'
  export default {
    props: ['namespace', 'workspace'],
    data() {
      return {
        ws: null,
        acl: null,
        acl_update: null,
        cache_state: null
      }
    },
    created() {
      this.getWorkspace(this.namespace, this.workspace);
      this.get_acl(this.namespace, this.workspace);
    },
    methods: {
      getWorkspace(namespace, workspace) {
        axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace)
          .then(response => {
            console.log(response.data);
            this.ws = response.data
            axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/cache')
              .then(response => {
                console.log(response.data);
                this.cache_state = response.data
              })
              .catch(error => {
                console.error("FAIL");
                console.error(response)
              })
          })
          .catch(error => {
            console.error("FAIL");
            console.error(response)
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
            console.error(response)
          })
      },

      get_entities(namespace, workspace) {
        axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace+'/entities')
          .then(response => {
            this.entities = response.data;
            for(let etype in this.entities.entity_types)
            {
              // this.get_cache_state(this.entities.entity_types[etype].type);
            }
          })
          .catch(error => {
            console.error("FAIL")
            console.error(response)
          })
      },
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
      this.getWorkspace(to.params.namespace, to.params.workspace);
      this.get_acl(to.params.namespace, to.params.workspace);
      next()
    }
  }
</script>
