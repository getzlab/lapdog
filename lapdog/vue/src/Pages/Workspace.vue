<template lang="html">
  <div id="workspace">
    <h4>{{namespace}}/{{workspace}}</h4>
    <div class="divider">

    </div>
    <div class="row">
      <div class="col s6">
        <h5><a v-bind:href="'https://portal.firecloud.org/#workspaces/'+namespace+'/'+workspace">Workspace Info</a></h5>
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
          <a v-bind:href="'https://accounts.google.com/AccountChooser?continue=https://console.cloud.google.com/storage/browser/'+ws.workspace.bucketName">
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
            <a class='btn blue'>Add Service Account</a>
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
        <div v-if="entities && entities.entity_types.length" class="col s6">
          <table>
            <thead>
              <tr>
                <th>Cache State</th>
                <th></th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="entity in entities.entity_types">
                <td>
                  {{entity.type}}
                </td>
                <td>
                  <span v-if="entity.cache == 'up-to-date'" class="green-text">
                    Up to date
                  </span>
                  <span v-if="entity.cache == 'sync'" class="cyan-text">
                    Syncing...
                  </span>
                  <span v-else-if="entity.cache == 'outdated'" class="amber-text text-darken-3">
                    Out of date
                  </span>
                  <span v-else-if="entity.cache == 'not-loaded'" class="red-text">
                    Not Loaded
                  </span>
                </td>
                <td>
                  <a class='btn blue' v-if="entity.cache != 'up-to-date' && entity.cache != 'sync'" v-on:click="sync_entities(entity.type)">Sync</a>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-if="entities && entities.entity_types.length">

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
        entities: null,
        cache_state: {}
      }
    },
    created() {
      this.getWorkspace(this.namespace, this.workspace);
      this.get_entities(this.namespace, this.workspace);
      this.get_acl(this.namespace, this.workspace);
    },
    methods: {
      getWorkspace(namespace, workspace) {
        axios.get('http://localhost:4201/api/v1/workspaces/'+namespace+'/'+workspace)
          .then(response => {
            console.log(response.data);
            this.ws = response.data
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
      sync_entities(etype) {
        for(let _etype in this.entities.entity_types)
        {
          if (this.entities.entity_types[_etype].type == etype)
          {
            this.entities.entity_types[_etype].cache = 'sync'
          }
        }
        axios.put('http://localhost:4201/api/v1/workspaces/'+this.namespace+'/'+this.workspace+'/cache/'+etype)
          .then(response => {
            for(let _etype in this.entities.entity_types)
            {
              if (this.entities.entity_types[_etype].type == etype)
              {
                this.entities.entity_types[_etype].cache = response.data
              }
            }
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
      this.get_entities(to.params.namespace, to.params.workspace);
      this.get_acl(to.params.namespace, to.params.workspace);
      next()
    }
  }
</script>
