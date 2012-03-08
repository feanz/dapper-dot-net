using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using PropertyAttributes = System.Reflection.PropertyAttributes;

namespace Dapper
{
    public static class SqlMapperOracleExtensions
    {
        private const string _sqlVariablePrefix = ":";

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> DeleteQueries =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> GetAllQueries =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> GetQueries =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties =
            new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<DataColumnInfo>> TypeProperties =
            new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<DataColumnInfo>>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<PropertyInfo, string> Sequences =
            new ConcurrentDictionary<PropertyInfo, string>();
        

        /// <summary>
        ///   Delete entity in table "Ts".
        /// </summary>
        /// <typeparam name="T"> Type of entity </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="entityToDelete"> Entity to delete </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> true if deleted, false if not found </returns>
        public static bool Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null,
                                     int? commandTimeout = null) where T : class
        {
            var type = typeof (T);

            var keyProperties = KeyPropertiesCache(type);
            if (keyProperties.Count() == 0)
                throw new ArgumentException("Entity must have at least one [Key] property");

            var tableName = GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", tableName);

            for (var i = 0; i < keyProperties.Count(); i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.AppendFormat("{0} = {1}{2}", property.Name, _sqlVariablePrefix, property.Name);
                if (i < keyProperties.Count() - 1)
                    sb.AppendFormat(" and ");
            }
            var deleted = connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
            return deleted > 0;
        }

        /// <summary>
        ///   Delete a single entity by key id
        /// </summary>
        /// <typeparam name="T"> Type of entity </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="id"> id to use to delete item by </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> true if deleted, false if not found </returns>
        public static bool Delete<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null,
                                     int? commandTimeout = null) where T : class
        {
            var type = typeof (T);

            string sql;
            if (!DeleteQueries.TryGetValue(type.TypeHandle, out sql))
            {
                var keys = KeyPropertiesCache(type);
                if (keys.Count() > 1)
                    throw new DataException("Get<T> only supports an entity with a single [Key] property");
                if (keys.Count() == 0)
                    throw new DataException("Get<T> only supports en entity with a [Key] property");

                var onlyKey = keys.First();

                var tableName = GetTableName(type);

                sql = string.Format("delete from {0} where {1} = {2}id", tableName, onlyKey.Name, _sqlVariablePrefix);
                DeleteQueries[type.TypeHandle] = sql;
            }

            var dynParms = new DynamicParameters();
            dynParms.Add(_sqlVariablePrefix + "id", id);

            var deleted = connection.Execute(sql, dynParms, transaction, commandTimeout);

            return deleted > 0;
        }

        /// <summary>
        ///   Delete all items in table
        /// </summary>
        /// <typeparam name="T"> Type of entity </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <returns> true if any deleted, false if none deleted </returns>
        public static bool DeleteAll<T>(this IDbConnection connection, IDbTransaction transaction = null)
            where T : class
        {
            var type = typeof (T);

            var tableName = GetTableName(type);

            var sql = string.Format("delete from {0}", tableName);

            var deleted = connection.Execute(sql, transaction);
            return deleted > 0;
        }

        /// <summary>
        ///   Returns a single entity by a single id from table "Ts". T can be an interface type, if so Id must be marked with [Key] attribute. Created entity is tracked/intercepted for changes and used by the Update() extension.
        /// </summary>
        /// <typeparam name="T"> Type to create and populate </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="id"> Id of the entity to get, must be marked with [Key] attribute </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> Entity of T </returns>
        public static T Get<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null,
                               int? commandTimeout = null) where T : class
        {
            var type = typeof (T);
            string sql;
            if (!GetQueries.TryGetValue(type.TypeHandle, out sql))
            {
                var keys = KeyPropertiesCache(type);
                if (keys.Count() > 1)
                    throw new DataException("Get<T> only supports an entity with a single [Key] property");
                if (keys.Count() == 0)
                    throw new DataException("Get<T> only supports en entity with a [Key] property");

                var onlyKey = keys.First();

                var name = GetTableName(type);

                sql = string.Format("select * from {0} where {1} = {2}id", name, onlyKey.Name, _sqlVariablePrefix);
                GetQueries[type.TypeHandle] = sql;
            }

            var dynParms = new DynamicParameters();
            dynParms.Add(_sqlVariablePrefix + "id", id);

            T obj;

            if (type.IsInterface)
            {
                var res = connection.Query(sql, dynParms).FirstOrDefault() as IDictionary<string, object>;

                if (res == null)
                    return null;

                obj = ProxyGenerator.GetInterfaceProxy<T>();

                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[property.PropertyInfo.Name];
                    property.PropertyInfo.SetValue(obj, val, null);
                }

                ((IProxy) obj).IsDirty = false; //reset change tracking and return
            }
            else
            {
                obj = connection.Query<T>(sql, dynParms, transaction, commandTimeout: commandTimeout).FirstOrDefault();
            }
            return obj;
        }

        /// <summary>
        ///   Return all entites from the table
        /// </summary>
        /// <typeparam name="T"> Interface type to create and populate </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> IEnumerable of T </returns>
        public static IEnumerable<T> GetAll<T>(this IDbConnection connection, IDbTransaction transaction = null,
                                               int? commandTimeout = null)
            where T : class
        {
            var type = typeof (T);

            string sql;
            if (!GetAllQueries.TryGetValue(type.TypeHandle, out sql))
            {
                var name = GetTableName(type);

                sql = "select * from " + name;

                GetAllQueries[type.TypeHandle] = sql;
            }

            return connection.Query<T>(sql, transaction, commandTimeout: commandTimeout);
        }

        /// <summary>
        ///   Return all entites using package
        /// </summary>
        /// <typeparam name="T"> Type to create and populate </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="packageName"> Package name to call </param>
        /// <param name="param"> Set of params to pass in, if none provided will added a default cursor </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> IEnumerable of T </returns>
        public static IEnumerable<T> GetFromPackage<T>(this IDbConnection connection, string packageName,
                                                       dynamic param = null, IDbTransaction transaction = null,
                                                       int? commandTimeout = null) where T : class
        {
            if (param == null)
            {
                param = new DynamicParameters();
                param.Add("CUR$", null, DbType.Object, ParameterDirection.Output, 0);
            }
            return connection.Query<T>(packageName, param as object, commandType: CommandType.StoredProcedure);
        }

        /// <summary>
        ///   Inserts an entity into table "Ts" and returns identity id.
        /// </summary>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="entityToInsert"> Entity to insert </param>
        /// <param name="transaction"> Transaction to use if provided </param>
        /// <param name="commandTimeout"> Optional command timeout </param>
        /// <returns> Identity of inserted entity </returns>
        public static decimal Insert<T>(this IDbConnection connection, T entityToInsert,
                                        IDbTransaction transaction = null,
                                        int? commandTimeout = null) where T : class
        {
            var type = typeof (T);

            var name = GetTableName(type);

            var sb = new StringBuilder(null);
            sb.AppendFormat("insert into {0} (", name);

            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var sequenceName = string.Empty;

            if (keyProperties.Any())
                sequenceName = GetSequenceName(keyProperties.FirstOrDefault());

            for (var i = 0; i < allProperties.Count(); i++)
            {
                var property = allProperties.ElementAt(i);

                sb.Append(property.ColumnName);
                if (i < allProperties.Count() - 1)
                    sb.Append(", ");
            }

            sb.Append(") values (");

            for (var i = 0; i < allProperties.Count(); i++)
            {
                var property = allProperties.ElementAt(i);
                if (property.ColumnName == keyProperties.FirstOrDefault().Name && !string.IsNullOrEmpty(sequenceName))
                    sb.AppendFormat("{0}.NEXTVAL", sequenceName);
                else
                    sb.AppendFormat("{0}{1}", _sqlVariablePrefix, property.PropertyInfo.Name);
                if (i < allProperties.Count() - 1)
                    sb.Append(", ");
            }

            sb.Append(") ");
            connection.Execute(sb.ToString(), entityToInsert, transaction, commandTimeout);

            //If we have a sequence name get the new identity and set object ident
            if (!String.IsNullOrEmpty(sequenceName))
            {
                var ident =
                    (decimal)
                    connection.Query("select " + sequenceName + ".CURRVAL from DUAL", transaction: transaction,
                                     commandTimeout: commandTimeout).First().CURRVAL;

                var key = keyProperties.FirstOrDefault();
                if (key != null)
                {
                    key.SetValue(entityToInsert, ident, null);
                }

                return ident;
            }

            return 0;
        }

        /// <summary>
        ///   Updates entity in table "Ts", checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T"> Type to be updated </typeparam>
        /// <param name="connection"> Open DBConnection </param>
        /// <param name="entityToUpdate"> Entity to be updated </param>
        /// <param name="transaction"> </param>
        /// <param name="commandTimeout"> </param>
        /// <returns> true if updated, false if not found or not modified (tracked entities) </returns>
        public static bool Update<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction transaction = null,
                                     int? commandTimeout = null) where T : class
        {
            var proxy = entityToUpdate as IProxy;
            if (proxy != null)
            {
                if (!proxy.IsDirty) return false;
            }

            var type = typeof (T);

            var keyProperties = KeyPropertiesCache(type);
            if (keyProperties.Count() == 0)
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", name);

            var allProperties = TypePropertiesCache(type);
            var nonIdProps = allProperties.Where(a => !keyProperties.Contains(a.PropertyInfo));

            for (var i = 0; i < nonIdProps.Count(); i++)
            {
                var property = nonIdProps.ElementAt(i);
                sb.AppendFormat("{0} = {1}{2}", property.ColumnName, _sqlVariablePrefix, property.PropertyInfo.Name);
                if (i < nonIdProps.Count() - 1)
                    sb.AppendFormat(", ");
            }
            sb.Append(" where ");
            for (var i = 0; i < keyProperties.Count(); i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.AppendFormat("{0} = {1}{2}", property.Name, _sqlVariablePrefix, property.Name);
                if (i < keyProperties.Count() - 1)
                    sb.AppendFormat(" and ");
            }
            var updated = connection.Execute(sb.ToString(), entityToUpdate, commandTimeout: commandTimeout,
                                             transaction: transaction);
            return updated > 0;
        }


        private static string GetSequenceName(PropertyInfo keyPropertie)
        {
            string sequence;
            if (!Sequences.TryGetValue(keyPropertie, out sequence))
            {
                var attribute = keyPropertie.GetCustomAttributes(typeof (KeyAttribute), false).FirstOrDefault();

                KeyAttribute keyAttribute = null;

                if (attribute != null)
                    keyAttribute = attribute as KeyAttribute;

                if (keyAttribute != null)
                    sequence = keyAttribute.SequenceName;

                Sequences[keyPropertie] = sequence;
            }
            return sequence;
        }

        private static string GetTableName(Type type)
        {
            string name;
            if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
            {
                name = type.Name;
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);

                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableattr =
                    type.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as
                    dynamic;
                if (tableattr != null)
                    name = tableattr.Name;


                TypeTableName[type.TypeHandle] = name;
            }
            return name;
        }

        private static IEnumerable<PropertyInfo> KeyPropertiesCache(Type type)
        {
            IEnumerable<PropertyInfo> pi;
            if (KeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                return pi;
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties =
                allProperties.Where(p => p.PropertyInfo.GetCustomAttributes(true).Any(a => a is KeyAttribute)).Select(
                    x => x.PropertyInfo).ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.FirstOrDefault(p => p.PropertyInfo.Name.ToLower() == "id").PropertyInfo;
                if (idProp != null)
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }

        private static IEnumerable<DataColumnInfo> TypePropertiesCache(Type type)
        {
            IEnumerable<DataColumnInfo> pis;
            if (TypeProperties.TryGetValue(type.TypeHandle, out pis))
            {
                return pis;
            }
            var properties =
                type.GetProperties().Where(
                    x =>
                    (x.PropertyType.IsValueType || x.PropertyType.IsEnum ||x.PropertyType.IsNullableType() 
                    || x.PropertyType.Name.ToLower().Contains("string")) && !x.PropertyType.Name.ToLower().Contains("bool"))
                    .Select(p => new DataColumnInfo
                                     {
                                         PropertyInfo = p,
                                         ColumnName =
                                             (p.GetCustomAttributes(typeof (ColumnNameAttribute), false).Count() == 1)
                                                 ? (p.GetCustomAttributes(typeof (ColumnNameAttribute), false).
                                                        FirstOrDefault() as ColumnNameAttribute).Name
                                                 : p.Name
                                     });

            return TypeProperties[type.TypeHandle] = properties;
        }

        #region Nested type: DataColumnInfo

        private class DataColumnInfo
        {
            public string ColumnName { get; set; }
            public PropertyInfo PropertyInfo { get; set; }
        }

        #endregion

        #region Nested type: IProxy

        public interface IProxy
        {
            bool IsDirty { get; set; }
        }

        #endregion

        #region Nested type: ProxyGenerator

        private class ProxyGenerator
        {
            private static readonly Dictionary<Type, object> TypeCache = new Dictionary<Type, object>();

            public static T GetClassProxy<T>()
            {
                // A class proxy could be implemented if all properties are virtual
                //  otherwise there is a pretty dangerous case where internal actions will not update dirty tracking
                throw new NotImplementedException();
            }

            public static T GetInterfaceProxy<T>()
            {
                Type typeOfT = typeof (T);

                object k;
                if (TypeCache.TryGetValue(typeOfT, out k))
                {
                    return (T) k;
                }
                var assemblyBuilder = GetAsmBuilder(typeOfT.Name);

                var moduleBuilder = assemblyBuilder.DefineDynamicModule("SqlMapperOracleExtensions." + typeOfT.Name);
                //NOTE: to save, add "asdasd.dll" parameter

                var interfaceType = typeof (IProxy);
                var typeBuilder = moduleBuilder.DefineType(typeOfT.Name + "_" + Guid.NewGuid(),
                                                           TypeAttributes.Public | TypeAttributes.Class);
                typeBuilder.AddInterfaceImplementation(typeOfT);
                typeBuilder.AddInterfaceImplementation(interfaceType);

                //create our _isDirty field, which implements IProxy
                var setIsDirtyMethod = CreateIsDirtyProperty(typeBuilder);

                // Generate a field for each property, which implements the T
                foreach (var property in typeof (T).GetProperties())
                {
                    var isId = property.GetCustomAttributes(true).Any(a => a is KeyAttribute);
                    CreateProperty<T>(typeBuilder, property.Name, property.PropertyType, setIsDirtyMethod, isId);
                }

                var generatedType = typeBuilder.CreateType();

                //assemblyBuilder.Save(name + ".dll");  //NOTE: to save, uncomment

                var generatedObject = Activator.CreateInstance(generatedType);

                TypeCache.Add(typeOfT, generatedObject);
                return (T) generatedObject;
            }

            private static MethodInfo CreateIsDirtyProperty(TypeBuilder typeBuilder)
            {
                var propType = typeof (bool);
                var field = typeBuilder.DefineField("_" + "IsDirty", propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty("IsDirty",
                                                          PropertyAttributes.None,
                                                          propType,
                                                          new[] {propType});

                const MethodAttributes getSetAttr =
                    MethodAttributes.Public | MethodAttributes.NewSlot | MethodAttributes.SpecialName |
                    MethodAttributes.Final | MethodAttributes.Virtual | MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + "IsDirty",
                                                                   getSetAttr,
                                                                   propType,
                                                                   Type.EmptyTypes);
                var currGetIL = currGetPropMthdBldr.GetILGenerator();
                currGetIL.Emit(OpCodes.Ldarg_0);
                currGetIL.Emit(OpCodes.Ldfld, field);
                currGetIL.Emit(OpCodes.Ret);
                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + "IsDirty",
                                                                   getSetAttr,
                                                                   null,
                                                                   new[] {propType});
                var currSetIL = currSetPropMthdBldr.GetILGenerator();
                currSetIL.Emit(OpCodes.Ldarg_0);
                currSetIL.Emit(OpCodes.Ldarg_1);
                currSetIL.Emit(OpCodes.Stfld, field);
                currSetIL.Emit(OpCodes.Ret);

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof (IProxy).GetMethod("get_" + "IsDirty");
                var setMethod = typeof (IProxy).GetMethod("set_" + "IsDirty");
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);

                return currSetPropMthdBldr;
            }

            private static void CreateProperty<T>(TypeBuilder typeBuilder, string propertyName, Type propType,
                                                  MethodInfo setIsDirtyMethod, bool isIdentity)
            {
                //Define the field and the property 
                var field = typeBuilder.DefineField("_" + propertyName, propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty(propertyName,
                                                          PropertyAttributes.None,
                                                          propType,
                                                          new[] {propType});

                const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.Virtual |
                                                    MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName,
                                                                   getSetAttr,
                                                                   propType,
                                                                   Type.EmptyTypes);

                var currGetIL = currGetPropMthdBldr.GetILGenerator();
                currGetIL.Emit(OpCodes.Ldarg_0);
                currGetIL.Emit(OpCodes.Ldfld, field);
                currGetIL.Emit(OpCodes.Ret);

                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
                                                                   getSetAttr,
                                                                   null,
                                                                   new[] {propType});

                //store value in private field and set the isdirty flag
                var currSetIL = currSetPropMthdBldr.GetILGenerator();
                currSetIL.Emit(OpCodes.Ldarg_0);
                currSetIL.Emit(OpCodes.Ldarg_1);
                currSetIL.Emit(OpCodes.Stfld, field);
                currSetIL.Emit(OpCodes.Ldarg_0);
                currSetIL.Emit(OpCodes.Ldc_I4_1);
                currSetIL.Emit(OpCodes.Call, setIsDirtyMethod);
                currSetIL.Emit(OpCodes.Ret);

                //TODO: Should copy all attributes defined by the interface?
                if (isIdentity)
                {
                    var keyAttribute = typeof (KeyAttribute);
                    var myConstructorInfo = keyAttribute.GetConstructor(new Type[] {});
                    var attributeBuilder = new CustomAttributeBuilder(myConstructorInfo, new object[] {});
                    property.SetCustomAttribute(attributeBuilder);
                }

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof (T).GetMethod("get_" + propertyName);
                var setMethod = typeof (T).GetMethod("set_" + propertyName);
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);
            }

            private static AssemblyBuilder GetAsmBuilder(string name)
            {
                var assemblyBuilder = Thread.GetDomain().DefineDynamicAssembly(new AssemblyName {Name = name},
                                                                               AssemblyBuilderAccess.Run);
                //NOTE: to save, use RunAndSave

                return assemblyBuilder;
            }
        }

        #endregion
    }

    public static class ReflectionExtensions
    {
        /// <summary>
        ///   Checks if type is nullable
        /// </summary>
        /// <param name="type"> Type to check </param>
        /// <returns> </returns>
        public static bool IsNullableType(this Type type)
        {
            return type.IsGenericType
                   && type.GetGenericTypeDefinition() == typeof (Nullable<>);
        }

        /// <summary>
        ///   Checks if property is nullable
        /// </summary>
        /// <param name="propertyInfo"> Property to check </param>
        /// <returns> </returns>
        public static bool IsNullableType(this PropertyInfo propertyInfo)
        {
            return propertyInfo.PropertyType.IsGenericType
                   && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof (Nullable<>);
        }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public class TableAttribute : Attribute
    {
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }

        public string Name { get; private set; }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public class KeyAttribute : Attribute
    {
        public KeyAttribute(string sequenceName)
        {
            SequenceName = sequenceName;
        }

        public string SequenceName { get; set; }
    }

    public class SqlBuilder
    {
        private readonly Dictionary<string, Clauses> data = new Dictionary<string, Clauses>();
        private int seq;

        public SqlBuilder AddParameters(dynamic parameters)
        {
            AddClause("--parameters", "", parameters, "");
            return this;
        }

        public Template AddTemplate(string sql, dynamic parameters = null)
        {
            return new Template(this, sql, parameters);
        }

        public SqlBuilder Join(string sql, dynamic parameters = null)
        {
            AddClause("join", sql, parameters, joiner: "\nJOIN ", prefix: "\nJOIN ", postfix: "\n");
            return this;
        }

        public SqlBuilder LeftJoin(string sql, dynamic parameters = null)
        {
            AddClause("leftjoin", sql, parameters, joiner: "\nLEFT JOIN ", prefix: "\nLEFT JOIN ", postfix: "\n");
            return this;
        }

        public SqlBuilder OrderBy(string sql, dynamic parameters = null)
        {
            AddClause("orderby", sql, parameters, " , ", prefix: "ORDER BY ", postfix: "\n");
            return this;
        }

        public SqlBuilder Select(string sql, dynamic parameters = null)
        {
            AddClause("select", sql, parameters, " , ", prefix: "", postfix: "\n");
            return this;
        }

        public SqlBuilder Where(string sql, dynamic parameters = null)
        {
            AddClause("where", sql, parameters, " AND ", prefix: "WHERE ", postfix: "\n");
            return this;
        }

        private void AddClause(string name, string sql, object parameters, string joiner, string prefix = "",
                               string postfix = "")
        {
            Clauses clauses;
            if (!data.TryGetValue(name, out clauses))
            {
                clauses = new Clauses(joiner, prefix, postfix);
                data[name] = clauses;
            }
            clauses.Add(new Clause {Sql = sql, Parameters = parameters});
            seq++;
        }

        #region Nested type: Clause

        private class Clause
        {
            public object Parameters { get; set; }
            public string Sql { get; set; }
        }

        #endregion

        #region Nested type: Clauses

        private class Clauses : List<Clause>
        {
            private readonly string joiner;
            private readonly string postfix;
            private readonly string prefix;

            public Clauses(string joiner, string prefix = "", string postfix = "")
            {
                this.joiner = joiner;
                this.prefix = prefix;
                this.postfix = postfix;
            }

            public string ResolveClauses(DynamicParameters p)
            {
                foreach (var item in this)
                {
                    p.AddDynamicParams(item.Parameters);
                }
                return prefix + string.Join(joiner, this.Select(c => c.Sql)) + postfix;
            }
        }

        #endregion

        #region Nested type: Template

        public class Template
        {
            private static readonly Regex regex =
                new Regex(@"\/\*\*.+\*\*\/", RegexOptions.Compiled | RegexOptions.Multiline);

            private readonly SqlBuilder builder;
            private readonly object initParams;
            private readonly string sql;
            private int dataSeq = -1; // Unresolved

            private object parameters;
            private string rawSql;

            public Template(SqlBuilder builder, string sql, dynamic parameters)
            {
                initParams = parameters;
                this.sql = sql;
                this.builder = builder;
            }

            public object Parameters
            {
                get
                {
                    ResolveSql();
                    return parameters;
                }
            }

            public string RawSql
            {
                get
                {
                    ResolveSql();
                    return rawSql;
                }
            }

            private void ResolveSql()
            {
                if (dataSeq != builder.seq)
                {
                    DynamicParameters p = new DynamicParameters(initParams);

                    rawSql = sql;

                    foreach (var pair in builder.data)
                    {
                        rawSql = rawSql.Replace("/**" + pair.Key + "**/", pair.Value.ResolveClauses(p));
                    }
                    parameters = p;

                    // replace all that is left with empty
                    rawSql = regex.Replace(rawSql, "");

                    dataSeq = builder.seq;
                }
            }
        }

        #endregion
    }
}