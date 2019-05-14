<template>
  <div id="app">
    <div class="modal" id="create-workspace-modal">
      <div class="modal-content">
        <div class="container form-container">
          <h4>Create Workspace</h4>
          <div class="row">
            <div class="input-field col s6">
              <input type="text" id="create-namespace" v-model="create_namespace">
              <label for="create-namespace">Namespace</label>
            </div>
            <div class="input-field col s6">
              <input type="text" id="create-workspace" v-model="create_workspace">
              <label for="create-nworkspace">Workspace</label>
            </div>
          </div>
          <div class="row">
            <div class="input-field col s12">
               <select id="clone-select" v-model="parent_workspace" class="browser-default">
                 <option value="" selected>Create empty workspace</option>
                 <option v-for="workspace in filtered_workspaces" v-bind:value="workspace.namespace+'/'+workspace.name" :key="workspace.namespace+'/'+workspace.name">
                   Clone from {{workspace.namespace+'/'+workspace.name}}
                 </option>
               </select>
             </div>
          </div>
          <div v-if="create_failed" class="row">
            <div class="col s12 red-text">
              <strong>{{create_failed}}</strong>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <a class="btn-flat" v-on:click="create_new_workspace">Create Workspace</a>
      </div>
    </div>
    <header>
      <div class="sidenav" id="slide-out" style="width: 50% !important;">
        <div class="user-view center-align">
          <h3>Workspaces</h3>
        </div>
        <div class="container">
          <div class="collection">
            <a href="#create-workspace-modal" class="collection-item blue-text text-darken-4 modal-trigger"><i class="material-icons">add_box</i> Create new Workspace</a>
          </div>
          <div class="divider">
          </div>
          <div class="collection">
            <div class="collection-item" v-if="!workspaces">
              <div class="progress">
                <div class="indeterminate blue"></div>
              </div>
            </div>
            <div class="collection-item" v-else>
              <div class="input-field">
                <input autocomplete="off" id="search_bar" type="text" v-model="search">
                <label for="search_bar">Search Workspaces</label>
              </div>
            </div>
            <router-link class="blue-text text-darken-4 collection-item sidenav-close" v-for="workspace in filtered_workspaces" active-class="active blue lighten-1t"
              :to="{name: 'workspace', params: {namespace: workspace.namespace, workspace: workspace.name}}" :key="workspace.namespace+'/'+workspace.name">
              {{workspace.namespace}}/{{workspace.name}}
            </router-link>
          </div>
        </div>
      </div>
      <div class="navbar-fixed">
        <nav class="blue darken-2">
          <div class="nav-wrapper">
            <div class="row">
              <div class="col s2">
                <ul class="left">
                  <li>
                    <a data-target="slide-out" class="sidenav-trigger show-on-large">Workspaces</a>
                    <a data-target="slide-out" class="sidenav-trigger show-on-medium-and-down"><i class="material-icons">menu</i></a>
                  </li>
                </ul>
              </div>
              <div class="col s6 offset-s1">
                <form v-on:submit.prevent="get_submission">
                  <div class="input-field">
                    <input autocomplete="off" type="text" id="submission-search" class="white" placeholder=" Jump to submission" v-model="submission"/>
                    <!-- <label for="submission-search">search</label> -->
                  </div>
                </form>
              </div>
              <div class="col s3">
                <router-link href="#" class="brand-logo right link" :to="'/'">Lapdog</router-link>
              </div>
            </div>
          </div>
        </nav>
      </div>
      <!-- <nav class="blue darken-2">
        <div class="nav-wrapper">
          <router-link href="#" class="brand-logo right link" :to="'/'">Lapdog</router-link>
          <ul class="left">
            <li>
              <a data-target="slide-out" class="sidenav-trigger show-on-large"><i class="material-icons">menu</i></a>
            </li>
          </ul>
        </div>
      </nav> -->
    </header>
    <main>
      <aside class="sidebar">
      </aside>
      <div class="content container">
        <div v-if="quotas && quotas.alerts.length" class="row" style="margin-top: 10px; border: 2px solid red;">
          <div class="col s10 offset s1 red-text pushpin">
            Alert: The following quotas may delay workflows: {{
              lodash.chain(quotas.alerts)
                .map(obj => obj.metric + ' (' + obj.usage + '/' + obj.limit + ')')
                .join(', ')
                .value()
            }}
          </div>
        </div>
        <div class="row" style="margin-top: 10px; border: 2px solid orange;">
          <div class="col s10 offset s1 orange-text">
            Lapdog is still in beta. Please submit any bug reports to the
            <a href="https://github.com/broadinstitute/lapdog/issues">Lapdog Github repository</a>
          </div>
        </div>
        <router-view @on-namespace-update="fetchQuotas"></router-view>
      </div>
    </main>
    <footer class="grey lighten-1">
      <div class="row">
        <div class="col s2 offset-s2">
          Powered by <a href="https://github.com/broadinstitute/dalmatian">Dalmatian</a>
        </div>
        <div v-if="acct" class="col s5">
          Your Lapdog service account: {{acct}}
        </div>
        <div v-if="cache_size" class="col s2">
          Offline Cache Size: {{cache_size}}
        </div>
      </div>
    </footer>
  </div>
</template>

<style lang="css">
   /* label focus color */
   .input-field input[type=text]:focus + label {
     color: #1565c0 !important;
   }
   /* label underline focus color */
   .input-field input[type=text]:focus {
     border-bottom: 1px solid #1565c0 !important;
     box-shadow: 0 1px 0 0 #000 !important;
   }
   /* valid color */
   .input-field input[type=text].valid {
     border-bottom: 1px solid #1565c0 !important;
     box-shadow: 0 1px 0 0 #000 !important;
   }
</style>

<script>
import axios from 'axios'
import _ from 'lodash'
export default {
  data() {
    return {
      lodash:_,
      workspaces: null,
      acct: null,
      search: '',
      submission: '',
      create_namespace: '',
      create_workspace: '',
      parent_workspace: '',
      create_failed: null,
      cache_size: null,
      quotas: null,
      namespace: null
    }
  },

  computed: {
    filtered_workspaces() {
      return !this.workspaces ? null : this.workspaces.filter(ws => {
        return this.search.length == 0 || (ws.namespace+'/'+ws.name).toLowerCase().includes(this.search.toLowerCase())
      })
    }
  },

  created() {
    this.getWorkspaces();
    this.getServiceAccount();
    axios.get(API_URL+'/api/v1/cache')
      .then(response => {
        this.cache_size = response.data;
        setInterval(() => {
          axios.get(API_URL+'/api/v1/cache')
            .then(response => {
              this.cache_size = response.data
            })
        }, 30000);
      })
      setInterval(() => {
        if (this.namespace) {
          axios.get(API_URL+'/api/v1/quotas/'+this.namespace)
            .then(response => {
              console.log("Quotas");
              console.log(response.data);
              this.quotas = response.data
            })
            .catch(error => {
              console.error("FAIL!");
              console.error(error);
              // Not toasting here. This would be super annoying
            });
        }
      }, 120000)
  },

  methods: {
    create_new_workspace(event) {
      let url = API_URL+'/api/v1/workspaces/'+this.create_namespace+'/'+this.create_workspace;
      if (this.parent_workspace.length > 1 && this.parent_workspace.includes('/')) {
        url += '?parent='+encodeURIComponent(this.parent_workspace);
      }
      window.materialize.toast({
        html: "Creating workspace..."
      });
      axios.post(url)
        .then(response => {
          if (response.data.failed) {
            this.create_failed = response.data.reason
          }
          else {
            window.$('.modal').modal('close');
            this.$router.push({
              name: 'workspace',
              params: {
                namespace: this.create_namespace,
                workspace: this.create_workspace
              }
            });
            this.create_namespace='';
            this.create_workspace='';
            this.parent_workspace='';
            this.create_failed=null;
            window.$('.sidenav').sidenav('close');

          }
        })
        .catch(response => {
          console.error("FAIL");
          console.error(response);
          window.materialize.toast({
            html: "Failed to create workspace"
          })
        })
    },

    getWorkspaces() {
      axios.get(API_URL+'/api/v1/workspaces')
        .then(response => {
          this.workspaces = _.filter(
            response.data,
            (workspace) => {return workspace.name != "do-not-delete-lapdog-resolution"}
          );
          // window.$('select').formSelect();
          console.log(this.workspaces[0])
        })
        .catch(error => {
          console.error("FAIL!");
          window.materialize.toast({
            html: "Failed to load workspaces"
          });
        })
    },
    get_submission() {
      if (!this.submission.startsWith('lapdog/')) {
        return window.materialize.toast({
          html: "Not a valid submission ID (must start with 'lapdog/')",
          displayLength: 5000,
        });
      }
      axios.get(API_URL+'/api/v1/submissions/decode?submission_id='+encodeURIComponent(this.submission))
        .then(response => {
          console.log("Decoded submission");
          this.submission = '';
          this.$router.push({
            name: 'submission',
            params: {
              namespace: response.data.namespace,
              workspace: response.data.workspace,
              submission_id: response.data.id
            }
          });
        })
        .catch(response => {
          window.materialize.toast({
            html: "No such submission: " + this.submission,
            displayLength: 5000,
          });
          console.error(response);
        })
    },
    getServiceAccount() {
      axios.get(API_URL+'/api/v1/service-account')
        .then(response => {
          this.acct = response.data
        })
        .catch(error => {
          console.error("FAIL!")
        })
    },
    fetchQuotas(namespace) {
      console.log("Updating Namespace");
      if (namespace != this.namespace) {
        this.quotas = null;
        if (namespace) axios.get(API_URL+'/api/v1/quotas/'+namespace)
          .then(response => {
            console.log("Quotas");
            console.log(response.data);
            this.quotas = response.data
          })
          .catch(error => {
            console.error("FAIL!");
            console.error(error);
            window.materialize.toast({
              html: "Unable to query quotas for namespace " + this.namespace
            })
          });
      };
      this.namespace = namespace;
    }
  }
}
</script>

<style lang="css">

.expandable {
  cursor: pointer;
}

</style>
